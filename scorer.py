"""
scorer.py – 최종 소싱 점수 산출 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.7

점수 체계 (100점 만점):
  가격경쟁력  30점 – 경쟁가 대비 우리 판매가 비율
  트렌드     30점 – 라쿠텐 랭킹 + 구글 트렌드 점수
  수요       25점 – 리뷰 수, 검색량, 출현 빈도
  플랫폼     15점 – 큐텐 내 경쟁 상황 (경쟁자 수, 가격 갭)

최종 필터: final_score ≥ 70 AND margin ≥ 20% AND 가격경쟁력 통과
"""

import math
import logging

logger = logging.getLogger(__name__)

# ── 점수 배점 ─────────────────────────────────────────────────
WEIGHT_PRICE    = 30
WEIGHT_TREND    = 30
WEIGHT_DEMAND   = 25
WEIGHT_PLATFORM = 15

# ── 필터 기준 ─────────────────────────────────────────────────
FINAL_SCORE_THRESHOLD   = 70
MIN_MARGIN_RATE         = 0.20
PRICE_RATIO_MAX         = 1.10   # 경쟁가 대비 110% 이하


# ══════════════════════════════════════════════════════════════
# 1. 개별 점수 계산
# ══════════════════════════════════════════════════════════════

def _score_price(item: dict) -> float:
    """
    가격경쟁력 점수 (30점 만점)
    - 경쟁가 대비 우리 가격이 저렴할수록 높은 점수
    - 경쟁가 없으면 마진율 기반 점수
    """
    sell_price = item.get("sell_price_jpy", 0)
    competitor_lowest = item.get("competitor_lowest_jpy", 0)
    margin_rate = item.get("margin_rate", 0)

    if sell_price <= 0:
        return 0

    # 경쟁가가 있는 경우: 비율 기반 점수
    if competitor_lowest > 0:
        ratio = sell_price / competitor_lowest

        if ratio <= 0.80:
            score = 30          # 20% 이상 저렴 → 만점
        elif ratio <= 0.90:
            score = 27          # 10~20% 저렴
        elif ratio <= 1.00:
            score = 24          # 동일 수준
        elif ratio <= 1.05:
            score = 20          # 5% 비쌈
        elif ratio <= 1.10:
            score = 15          # 10% 비쌈 (통과 경계)
        elif ratio <= 1.20:
            score = 8           # 20% 비쌈
        else:
            score = 3           # 20% 초과 비쌈

        return score

    # 경쟁가 없는 경우: 마진율 기반 보정
    if margin_rate >= 0.30:
        return 20       # 마진 충분 → 가격 여유 있음
    elif margin_rate >= 0.20:
        return 15       # 기본 마진 충족
    else:
        return 5


def _score_trend(item: dict) -> float:
    """
    트렌드 점수 (30점 만점)
    - 라쿠텐 랭킹 순위 + 구글 트렌드 관심도/급상승
    """
    combined_score = item.get("combined_score", 0)
    demand_rank = item.get("demand_rank", 999)

    # combined_score 기반 (trend_analyzer에서 산출된 값)
    if combined_score > 0:
        # combined_score는 최대 ~50 정도 (라쿠텐30 + 구글25)
        normalized = min(combined_score / 50 * WEIGHT_TREND, WEIGHT_TREND)
        return round(normalized, 1)

    # demand_rank 기반 폴백
    if demand_rank <= 5:
        return 28
    elif demand_rank <= 10:
        return 24
    elif demand_rank <= 20:
        return 18
    elif demand_rank <= 50:
        return 12
    else:
        return 5


def _score_demand(item: dict) -> float:
    """
    수요 점수 (25점 만점)
    - 리뷰 수 (라쿠텐 기준)
    - 검색 결과 수 (경쟁가 조회 시 item_count)
    - 트렌드 키워드 출현 빈도
    """
    score = 0

    # 1. 리뷰 수 기반 (최대 12점)
    total_reviews = item.get("total_reviews", 0)
    competitor_info = item.get("competitor_info", {})
    rakuten_info = competitor_info.get("rakuten", {})
    rakuten_reviews = 0

    # 라쿠텐 검색 결과 내 리뷰 합산
    for top_item in rakuten_info.get("top_items", []):
        rakuten_reviews += top_item.get("reviews", 0)

    reviews = max(total_reviews, rakuten_reviews)
    if reviews > 0:
        review_score = min(12, math.log10(max(reviews, 1)) * 4)
        score += review_score

    # 2. 경쟁가 검색 결과 수 기반 (최대 8점)
    rakuten_count = rakuten_info.get("item_count", 0)
    kakaku_info = competitor_info.get("kakaku", {})
    kakaku_count = kakaku_info.get("item_count", 0)
    total_listings = rakuten_count + kakaku_count

    if total_listings >= 30:
        score += 8      # 많은 판매처 = 높은 수요
    elif total_listings >= 15:
        score += 6
    elif total_listings >= 5:
        score += 4
    elif total_listings >= 1:
        score += 2

    # 3. 출현 빈도 기반 (최대 5점)
    appearance = item.get("appearance_count", 0)
    if appearance >= 5:
        score += 5
    elif appearance >= 3:
        score += 3
    elif appearance >= 1:
        score += 1

    return min(round(score, 1), WEIGHT_DEMAND)


def _score_platform(item: dict) -> float:
    """
    플랫폼 점수 (15점 만점)
    - 큐텐 내 경쟁 상황: 경쟁자가 적을수록 유리
    - 카카쿠에 큐텐 판매자 미포함 시 보너스
    """
    score = 0
    competitor_info = item.get("competitor_info", {})

    # 1. 큐텐 경쟁 상황 (최대 10점)
    qoo10_info = competitor_info.get("qoo10")
    if qoo10_info is None:
        # 큐텐 조회 안 함 (카카쿠에 포함)
        kakaku_info = competitor_info.get("kakaku", {})
        if kakaku_info.get("has_qoo10"):
            score += 5      # 큐텐에 이미 있음 → 보통
        else:
            score += 10     # 큐텐에 없음 → 블루오션
    else:
        qoo10_count = qoo10_info.get("item_count", 0)
        if qoo10_count == 0:
            score += 10     # 큐텐 경쟁자 없음 → 블루오션
        elif qoo10_count <= 3:
            score += 7      # 소수 경쟁
        elif qoo10_count <= 10:
            score += 4      # 보통 경쟁
        else:
            score += 1      # 과다 경쟁

    # 2. 가격 갭 보너스 (최대 5점)
    sell_price = item.get("sell_price_jpy", 0)
    qoo10_lowest = 0
    if qoo10_info and qoo10_info.get("lowest_price", 0) > 0:
        qoo10_lowest = qoo10_info["lowest_price"]
    elif competitor_info.get("kakaku", {}).get("has_qoo10"):
        # 카카쿠에서 큐텐 가격을 직접 알 수 없으므로 전체 최저가 참조
        pass

    if sell_price > 0 and qoo10_lowest > 0:
        gap_ratio = sell_price / qoo10_lowest
        if gap_ratio <= 0.90:
            score += 5      # 큐텐 내 최저가보다 10% 이상 저렴
        elif gap_ratio <= 1.00:
            score += 3      # 동일 수준
        elif gap_ratio <= 1.10:
            score += 1
    elif qoo10_lowest == 0:
        score += 3          # 큐텐 가격 없음 → 가격 비교 불가 → 중립

    return min(round(score, 1), WEIGHT_PLATFORM)


# ══════════════════════════════════════════════════════════════
# 2. 종합 점수 계산
# ══════════════════════════════════════════════════════════════

def calculate_score(item: dict) -> dict:
    """
    개별 상품의 종합 점수 산출
    Returns:
        {
            "price_score": 24,
            "trend_score": 28,
            "demand_score": 18,
            "platform_score": 10,
            "final_score": 80,
            "grade": "A",
            "pass_final": True,
        }
    """
    price_score    = _score_price(item)
    trend_score    = _score_trend(item)
    demand_score   = _score_demand(item)
    platform_score = _score_platform(item)

    final_score = round(price_score + trend_score + demand_score + platform_score, 1)

    # 등급 부여
    if final_score >= 85:
        grade = "S"
    elif final_score >= 75:
        grade = "A"
    elif final_score >= 65:
        grade = "B"
    elif final_score >= 50:
        grade = "C"
    else:
        grade = "D"

    # 최종 필터 통과 여부
    margin_rate = item.get("margin_rate", 0)
    sell_price = item.get("sell_price_jpy", 0)
    competitor_lowest = item.get("competitor_lowest_jpy", 0)

    price_competitive = True
    if competitor_lowest > 0 and sell_price > 0:
        price_competitive = (sell_price / competitor_lowest) <= PRICE_RATIO_MAX

    pass_final = (
        final_score >= FINAL_SCORE_THRESHOLD
        and margin_rate >= MIN_MARGIN_RATE
        and price_competitive
    )

    return {
        "price_score":    price_score,
        "trend_score":    trend_score,
        "demand_score":   demand_score,
        "platform_score": platform_score,
        "final_score":    final_score,
        "grade":          grade,
        "pass_final":     pass_final,
    }


def calculate_scores_batch(items: list) -> list:
    """
    상품 리스트 일괄 점수 계산
    Returns:
        items에 score_info 필드가 추가된 리스트 (final_score 내림차순 정렬)
    """
    for item in items:
        score_info = calculate_score(item)
        item["score_info"]    = score_info
        item["final_score"]   = score_info["final_score"]
        item["grade"]         = score_info["grade"]
        item["pass_final"]    = score_info["pass_final"]

    # final_score 내림차순 정렬
    items.sort(key=lambda x: x.get("final_score", 0), reverse=True)

    # 통계
    total = len(items)
    passed = sum(1 for item in items if item.get("pass_final"))
    grades = {}
    for item in items:
        g = item.get("grade", "D")
        grades[g] = grades.get(g, 0) + 1

    logger.info(
        f"[스코어링] {total}건 완료 → "
        f"통과 {passed}건, "
        f"등급분포: S={grades.get('S',0)} A={grades.get('A',0)} "
        f"B={grades.get('B',0)} C={grades.get('C',0)} D={grades.get('D',0)}"
    )

    return items


def get_final_candidates(items: list) -> list:
    """
    최종 필터 통과 상품만 추출
    Returns:
        pass_final == True인 상품 리스트
    """
    candidates = [item for item in items if item.get("pass_final")]
    logger.info(f"[최종후보] {len(items)}건 → {len(candidates)}건 통과")
    return candidates


# ══════════════════════════════════════════════════════════════
# 직접 실행 (테스트)
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # 테스트 데이터
    test_items = [
        {
            "name": "ANUA 어성초 토너",
            "sell_price_jpy": 2600,
            "margin_rate": 0.217,
            "competitor_lowest_jpy": 2500,
            "combined_score": 42,
            "demand_rank": 3,
            "total_reviews": 1200,
            "appearance_count": 5,
            "competitor_info": {
                "rakuten": {"item_count": 25, "top_items": [{"reviews": 500}]},
                "kakaku": {"item_count": 12, "has_qoo10": False},
                "qoo10": {"item_count": 2, "lowest_price": 2800},
            },
        },
        {
            "name": "TIRTIR 쿠션파운데이션",
            "sell_price_jpy": 3200,
            "margin_rate": 0.22,
            "competitor_lowest_jpy": 2800,
            "combined_score": 38,
            "demand_rank": 7,
            "total_reviews": 800,
            "appearance_count": 3,
            "competitor_info": {
                "rakuten": {"item_count": 30, "top_items": [{"reviews": 300}]},
                "kakaku": {"item_count": 18, "has_qoo10": True},
                "qoo10": None,
            },
        },
        {
            "name": "무명 브랜드 크림",
            "sell_price_jpy": 1800,
            "margin_rate": 0.15,
            "competitor_lowest_jpy": 1200,
            "combined_score": 8,
            "demand_rank": 45,
            "total_reviews": 10,
            "appearance_count": 0,
            "competitor_info": {
                "rakuten": {"item_count": 2, "top_items": []},
                "kakaku": {"item_count": 0, "has_qoo10": False},
                "qoo10": {"item_count": 0, "lowest_price": 0},
            },
        },
    ]

    results = calculate_scores_batch(test_items)

    print("\n" + "=" * 80)
    print(f"{'상품명':<24} {'가격':>4} {'트렌드':>6} {'수요':>4} {'플랫폼':>6} {'합계':>4} {'등급':>4} {'통과'}")
    print("-" * 80)

    for item in results:
        si = item["score_info"]
        status = "✅" if item["pass_final"] else "❌"
        print(
            f"{item['name']:<24} "
            f"{si['price_score']:>5.0f} "
            f"{si['trend_score']:>6.1f} "
            f"{si['demand_score']:>5.1f} "
            f"{si['platform_score']:>5.1f} "
            f"{si['final_score']:>5.1f} "
            f"{si['grade']:>4} "
            f"{status}"
        )
    print("=" * 80)
