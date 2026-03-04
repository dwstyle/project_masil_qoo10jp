"""
price_calculator.py – 원가 계산 & 판매가 역산 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.7

공식:
  1. 총원가(KRW) = 공급가 + 국내배송비 + 수출신고비(150원)
  2. 총원가(JPY) = 총원가(KRW) / EXCHANGE_RATE
  3. KSE 국제배송비(JPY) = 요율표 lookup + 버퍼(100엔)
  4. 총비용(JPY) = 총원가(JPY) + KSE배송비(JPY)
  5. 판매가(JPY) = 총비용 / (1 - 목표마진율 - 큐텐수수료율)
  6. 판매가 → 100엔 단위 올림
  7. 실제마진 = 판매가 - 큐텐수수료 - 총비용
"""

import json
import math
import logging

logger = logging.getLogger(__name__)

# ── 상수 ──────────────────────────────────────────────────────
TARGET_MARGIN_RATE  = 0.20      # 목표 마진율 20%
QOO10_FEE_RATE      = 0.12      # 큐텐 수수료 12%
EXCHANGE_RATE       = 10        # 1 JPY = 10 KRW (100 JPY = 1,000 KRW)
EXPORT_FEE_KRW      = 150       # 수출신고비 (원)
KSE_BUFFER_JPY      = 100       # 배송비 버퍼 (엔)
PRICE_ROUND_UNIT    = 100       # 판매가 올림 단위 (엔)
DIVISOR             = 1 - TARGET_MARGIN_RATE - QOO10_FEE_RATE  # 0.68

# ── KSE 요율표 로드 ──────────────────────────────────────────
_kse_data = None

def _load_kse_rates():
    """kse_shipping_rates.json 로드 (1회)"""
    global _kse_data
    if _kse_data is not None:
        return _kse_data
    try:
        with open("kse_shipping_rates.json", "r", encoding="utf-8") as f:
            _kse_data = json.load(f)
        logger.info("KSE 요율표 로드 완료")
    except FileNotFoundError:
        logger.warning("kse_shipping_rates.json 미발견 – 기본값 사용")
        _kse_data = _default_kse_data()
    return _kse_data


def _default_kse_data():
    """JSON 파일 미발견 시 기본 요율"""
    return {
        "light_rates": {"0.50": 460},
        "sagawa_rates": {"0.50": 693, "1.00": 835},
        "buffer_jpy": 100,
        "category_weight_map": {
            "default": {"weight_kg": 0.50, "carrier": "light"}
        }
    }


# ══════════════════════════════════════════════════════════════
# 1. KSE 배송비 조회
# ══════════════════════════════════════════════════════════════

def get_kse_shipping(category: str = "default", weight_kg: float = None) -> dict:
    """
    KSE 국제배송비 조회
    Args:
        category: KJ9603 중분류명 (예: "스킨케어", "립메이크업")
        weight_kg: 직접 지정 시 카테고리 무게 무시
    Returns:
        {
            "carrier": "light" or "sagawa",
            "weight_kg": 0.50,
            "base_fee_jpy": 460,
            "buffer_jpy": 100,
            "total_fee_jpy": 560,
        }
    """
    kse = _load_kse_rates()
    cat_map = kse.get("category_weight_map", {})
    buffer  = kse.get("buffer_jpy", KSE_BUFFER_JPY)

    # 카테고리 매핑에서 무게·배송사 결정
    cat_info = cat_map.get(category, cat_map.get("default", {"weight_kg": 0.50, "carrier": "light"}))
    carrier  = cat_info["carrier"]
    w_kg     = weight_kg if weight_kg is not None else cat_info["weight_kg"]

    # 요율표에서 요금 조회
    if carrier == "light":
        rates = kse.get("light_rates", {})
    else:
        rates = kse.get("sagawa_rates", {})

    # 가장 가까운 무게 구간 찾기 (올림)
    weight_keys = sorted([float(k) for k in rates.keys()])
    selected_key = weight_keys[-1]  # 기본: 최대값
    for wk in weight_keys:
        if wk >= w_kg:
            selected_key = wk
            break

    base_fee = rates.get(f"{selected_key:.2f}", rates.get(str(selected_key), 460))

    return {
        "carrier":       carrier,
        "weight_kg":     w_kg,
        "base_fee_jpy":  base_fee,
        "buffer_jpy":    buffer,
        "total_fee_jpy": base_fee + buffer,
    }


# ══════════════════════════════════════════════════════════════
# 2. 판매가 계산
# ══════════════════════════════════════════════════════════════

def calculate_price(supply_price_krw: int, kj_shipping_krw: int = 3500,
                    category: str = "default", weight_kg: float = None) -> dict:
    """
    전체 가격 계산 파이프라인

    Args:
        supply_price_krw: 공급가 (원) – KJ9603 회원가
        kj_shipping_krw:  국내배송비 (원) – 기본 3,500원
        category:         KJ9603 중분류명
        weight_kg:        직접 지정 시 카테고리 무게 무시

    Returns:
        {
            "supply_price_krw":     8000,
            "kj_shipping_krw":      3500,
            "export_fee_krw":       150,
            "total_cost_krw":       11650,
            "total_cost_jpy":       1165,
            "kse_shipping":         { ... },
            "total_landed_jpy":     1725,
            "sell_price_jpy":       2600,
            "qoo10_fee_jpy":        312,
            "margin_jpy":           563,
            "margin_rate":          0.217,
            "is_profitable":        True,
        }
    """
    # STEP 1: 총원가 (KRW)
    total_cost_krw = supply_price_krw + kj_shipping_krw + EXPORT_FEE_KRW

    # STEP 2: 환산 (JPY)
    total_cost_jpy = total_cost_krw / EXCHANGE_RATE

    # STEP 3: KSE 국제배송비
    kse = get_kse_shipping(category, weight_kg)
    kse_fee_jpy = kse["total_fee_jpy"]

    # STEP 4: 총 비용 (JPY)
    total_landed_jpy = total_cost_jpy + kse_fee_jpy

    # STEP 5: 판매가 역산
    raw_sell_price = total_landed_jpy / DIVISOR

    # STEP 6: 100엔 올림
    sell_price_jpy = math.ceil(raw_sell_price / PRICE_ROUND_UNIT) * PRICE_ROUND_UNIT

    # STEP 7: 실제 마진 계산
    qoo10_fee_jpy = round(sell_price_jpy * QOO10_FEE_RATE)
    margin_jpy    = sell_price_jpy - qoo10_fee_jpy - total_landed_jpy
    margin_rate   = margin_jpy / sell_price_jpy if sell_price_jpy > 0 else 0

    return {
        "supply_price_krw":     supply_price_krw,
        "kj_shipping_krw":      kj_shipping_krw,
        "export_fee_krw":       EXPORT_FEE_KRW,
        "total_cost_krw":       total_cost_krw,
        "total_cost_jpy":       round(total_cost_jpy, 1),
        "kse_shipping":         kse,
        "kse_fee_jpy":          kse_fee_jpy,
        "total_landed_jpy":     round(total_landed_jpy, 1),
        "raw_sell_price_jpy":   round(raw_sell_price, 1),
        "sell_price_jpy":       sell_price_jpy,
        "qoo10_fee_jpy":        qoo10_fee_jpy,
        "margin_jpy":           round(margin_jpy, 1),
        "margin_rate":          round(margin_rate, 4),
        "is_profitable":        margin_rate >= TARGET_MARGIN_RATE,
    }


# ══════════════════════════════════════════════════════════════
# 3. 일괄 계산 (PASS 2)
# ══════════════════════════════════════════════════════════════

def calculate_prices_batch(items: list, category: str = "default") -> list:
    """
    상품 리스트에 대해 일괄 가격 계산
    Args:
        items: crawler_kj에서 수집된 상품 리스트
        category: 기본 카테고리 (개별 상품에 category 필드가 있으면 그것 사용)
    Returns:
        items 리스트에 price_info 필드가 추가된 리스트
    """
    calculated = []
    profitable_count = 0

    for item in items:
        cat = item.get("category", category)
        supply = item.get("supply_price", 0)
        shipping = item.get("kj_shipping", 3500)

        if supply <= 0:
            item["price_info"] = None
            item["is_profitable"] = False
            calculated.append(item)
            continue

        price_info = calculate_price(
            supply_price_krw=supply,
            kj_shipping_krw=shipping,
            category=cat,
        )
        item["price_info"] = price_info
        item["is_profitable"] = price_info["is_profitable"]
        item["sell_price_jpy"] = price_info["sell_price_jpy"]
        item["margin_rate"] = price_info["margin_rate"]

        if price_info["is_profitable"]:
            profitable_count += 1

        calculated.append(item)

    logger.info(f"[가격계산] {len(items)}건 중 {profitable_count}건 수익성 충족 (마진≥{TARGET_MARGIN_RATE*100}%)")
    return calculated


# ══════════════════════════════════════════════════════════════
# 4. 경쟁가 대비 검증
# ══════════════════════════════════════════════════════════════

COMPETITOR_PRICE_THRESHOLD = 1.10  # 경쟁가 대비 110% 이하

def check_price_competitiveness(item: dict, competitor_price_jpy: int) -> dict:
    """
    경쟁가 대비 가격 경쟁력 확인
    Args:
        item: 가격 계산 완료된 상품 dict
        competitor_price_jpy: 경쟁 최저가 (엔)
    Returns:
        {
            "competitor_price_jpy": 2500,
            "our_price_jpy": 2600,
            "price_ratio": 1.04,
            "is_competitive": True,
            "price_gap_jpy": 100,
        }
    """
    our_price = item.get("sell_price_jpy", 0)

    if competitor_price_jpy <= 0 or our_price <= 0:
        return {
            "competitor_price_jpy": competitor_price_jpy,
            "our_price_jpy":        our_price,
            "price_ratio":          0,
            "is_competitive":       True,  # 경쟁가 없으면 통과
            "price_gap_jpy":        0,
        }

    price_ratio = our_price / competitor_price_jpy

    return {
        "competitor_price_jpy": competitor_price_jpy,
        "our_price_jpy":        our_price,
        "price_ratio":          round(price_ratio, 3),
        "is_competitive":       price_ratio <= COMPETITOR_PRICE_THRESHOLD,
        "price_gap_jpy":        our_price - competitor_price_jpy,
    }


# ══════════════════════════════════════════════════════════════
# 직접 실행 (테스트)
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # 테스트 케이스
    test_cases = [
        {"name": "립 메이크업",   "supply": 5000,  "shipping": 3500, "category": "립메이크업"},
        {"name": "스킨케어 토너", "supply": 8000,  "shipping": 3500, "category": "스킨케어"},
        {"name": "헤어 샴푸",     "supply": 12000, "shipping": 3500, "category": "헤어샴푸"},
        {"name": "세트/키트",     "supply": 25000, "shipping": 0,    "category": "세트/키트"},
        {"name": "세럼 (고가)",   "supply": 50000, "shipping": 3500, "category": "세럼/에센스"},
    ]

    print("\n" + "=" * 80)
    print(f"{'상품명':<16} {'공급가':>8} {'배송비':>6} {'총원가JPY':>9} {'KSE':>5} {'판매가':>7} {'마진':>7} {'마진율':>6} {'결과'}")
    print("-" * 80)

    for tc in test_cases:
        result = calculate_price(
            supply_price_krw=tc["supply"],
            kj_shipping_krw=tc["shipping"],
            category=tc["category"],
        )
        status = "✅ OK" if result["is_profitable"] else "❌ NG"
        print(
            f"{tc['name']:<16} "
            f"{tc['supply']:>7,}원 "
            f"{tc['shipping']:>5,}원 "
            f"¥{result['total_cost_jpy']:>7,} "
            f"¥{result['kse_fee_jpy']:>4,} "
            f"¥{result['sell_price_jpy']:>6,} "
            f"¥{result['margin_jpy']:>6,} "
            f"{result['margin_rate']*100:>5.1f}% "
            f"{status}"
        )

    print("=" * 80)
