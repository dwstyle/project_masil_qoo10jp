"""
competitor_price.py – 일본 경쟁가 조회 모듈 (3단계)
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.7

3단계 경쟁가 조회 전략:
  STEP 1: 라쿠텐 상품검색 API (무료, ~150건, 3분)
  STEP 2: 카카쿠닷컴 크롤링 (무료, ~50건, 6분)
  STEP 3: 큐텐 크롤링 (카카쿠 미포함 시만, ~10건)
"""

import os
import re
import time
import random
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote

logger = logging.getLogger(__name__)

# ── 라쿠텐 API ────────────────────────────────────────────────
RAKUTEN_APP_ID     = os.environ.get("RAKUTEN_APP_ID", "")
RAKUTEN_ACCESS_KEY = os.environ.get("RAKUTEN_ACCESS_KEY", "")
RAKUTEN_SEARCH_URL = "https://openapi.rakuten.co.jp/ichiba/api/IchibaItem/Search/20220601"

# ── 카카쿠닷컴 ────────────────────────────────────────────────
KAKAKU_SEARCH_URL = "https://search.kakaku.com/search_results/"

# ── 딜레이 설정 ───────────────────────────────────────────────
RAKUTEN_API_DELAY  = 1.1       # 초당 1회 제한
KAKAKU_DELAY_MIN   = 5         # 카카쿠 최소 딜레이 (초)
KAKAKU_DELAY_MAX   = 10        # 카카쿠 최대 딜레이 (초)
QOO10_DELAY_MIN    = 3
QOO10_DELAY_MAX    = 7

# ── User-Agent ────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
}


# ══════════════════════════════════════════════════════════════
# STEP 1: 라쿠텐 상품검색 API
# ══════════════════════════════════════════════════════════════

def search_rakuten_price(keyword_jp: str, genre_id: int = 100939) -> dict:
    """
    라쿠텐 상품검색 API로 경쟁가 조회
    Args:
        keyword_jp: 일본어 검색 키워드 (상품명 또는 브랜드+상품)
        genre_id: 뷰티 장르 100939
    Returns:
        {
            "source": "rakuten",
            "lowest_price": 2500,
            "avg_price": 2800,
            "item_count": 15,
            "top_items": [ { "name", "price", "shop", "url", "reviews" }, ... ]
        }
    """
    if not RAKUTEN_APP_ID:
        logger.warning("[라쿠텐] APP_ID 미설정")
        return _empty_result("rakuten")

    params = {
        "applicationId": RAKUTEN_APP_ID,
        "keyword": keyword_jp,
        "genreId": genre_id,
        "hits": 30,
        "sort": "+itemPrice",  # 가격 낮은 순
        "formatVersion": 2,
    }
    if RAKUTEN_ACCESS_KEY:
        params["accessKey"] = RAKUTEN_ACCESS_KEY

    time.sleep(RAKUTEN_API_DELAY)

    try:
        resp = requests.get(RAKUTEN_SEARCH_URL, params=params, timeout=15)

        if resp.status_code == 429:
            logger.warning("[라쿠텐] 429 Too Many Requests – 30초 대기")
            time.sleep(30)
            resp = requests.get(RAKUTEN_SEARCH_URL, params=params, timeout=15)

        if resp.status_code != 200:
            logger.warning(f"[라쿠텐] HTTP {resp.status_code}")
            return _empty_result("rakuten")

        data = resp.json()
        items = data.get("Items", [])

        if not items:
            logger.debug(f"[라쿠텐] '{keyword_jp}' 검색 결과 없음")
            return _empty_result("rakuten")

        prices = []
        top_items = []
        for item in items:
            price = item.get("itemPrice", 0)
            if price > 0:
                prices.append(price)
            if len(top_items) < 5:
                top_items.append({
                    "name":    item.get("itemName", "")[:80],
                    "price":   price,
                    "shop":    item.get("shopName", ""),
                    "url":     item.get("itemUrl", ""),
                    "reviews": item.get("reviewCount", 0),
                })

        lowest = min(prices) if prices else 0
        avg    = round(sum(prices) / len(prices)) if prices else 0

        logger.info(f"[라쿠텐] '{keyword_jp}' → 최저 ¥{lowest:,}, 평균 ¥{avg:,}, {len(prices)}건")

        return {
            "source":       "rakuten",
            "lowest_price": lowest,
            "avg_price":    avg,
            "item_count":   len(prices),
            "top_items":    top_items,
        }

    except Exception as e:
        logger.error(f"[라쿠텐] 검색 오류 ({keyword_jp}): {e}")
        return _empty_result("rakuten")


# ══════════════════════════════════════════════════════════════
# STEP 2: 카카쿠닷컴 크롤링
# ══════════════════════════════════════════════════════════════

def search_kakaku_price(keyword_jp: str) -> dict:
    """
    카카쿠닷컴 검색 결과에서 최저가·판매처 수집
    Args:
        keyword_jp: 일본어 검색 키워드
    Returns:
        {
            "source": "kakaku",
            "lowest_price": 2300,
            "avg_price": 2600,
            "item_count": 8,
            "sellers": ["Amazon", "楽天", "Yahoo"],
            "has_qoo10": False,
            "top_items": [ ... ]
        }
    """
    delay = random.uniform(KAKAKU_DELAY_MIN, KAKAKU_DELAY_MAX)
    time.sleep(delay)

    url = f"{KAKAKU_SEARCH_URL}?query={quote(keyword_jp)}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)

        if resp.status_code != 200:
            logger.warning(f"[카카쿠] HTTP {resp.status_code} for '{keyword_jp}'")
            return _empty_result("kakaku")

        soup = BeautifulSoup(resp.text, "lxml")

        # 검색 결과 상품 리스트 파싱
        prices = []
        sellers = set()
        top_items = []
        has_qoo10 = False

        # 카카쿠닷컴 검색 결과 구조 파싱
        # 주요 셀렉터: 상품 카드 내 가격 정보
        product_cards = soup.select("div.p-result_list_item, div.p-list_item, li.p-result_item")

        if not product_cards:
            # 대체 셀렉터
            product_cards = soup.select("div[class*='result'] div[class*='item']")

        for card in product_cards[:20]:
            # 가격 추출
            price_el = card.select_one(
                "span.p-result_price, span.p-item_price, "
                "span[class*='price'], p[class*='price']"
            )
            if price_el:
                price_text = price_el.get_text(strip=True)
                price = _extract_yen_price(price_text)
                if price > 0:
                    prices.append(price)

            # 상품명 추출
            name_el = card.select_one(
                "a.p-result_title, a.p-item_title, "
                "a[class*='title'], h3 a"
            )
            item_name = name_el.get_text(strip=True) if name_el else ""
            item_url  = name_el.get("href", "") if name_el else ""

            # 판매처 추출
            shop_els = card.select("span[class*='shop'], a[class*='shop']")
            for shop_el in shop_els:
                shop_name = shop_el.get_text(strip=True)
                if shop_name:
                    sellers.add(shop_name)
                    if "qoo10" in shop_name.lower() or "キューテン" in shop_name:
                        has_qoo10 = True

            if item_name and len(top_items) < 5:
                top_items.append({
                    "name":  item_name[:80],
                    "price": prices[-1] if prices else 0,
                    "url":   item_url,
                })

        # 텍스트 전체에서 가격 추출 (셀렉터 실패 시 폴백)
        if not prices:
            all_text = soup.get_text()
            yen_matches = re.findall(r"¥([\d,]+)", all_text)
            for m in yen_matches[:10]:
                p = int(m.replace(",", ""))
                if 100 < p < 500000:
                    prices.append(p)

        # Qoo10 포함 여부 텍스트 검색
        if not has_qoo10:
            page_text = soup.get_text().lower()
            if "qoo10" in page_text or "キューテン" in page_text:
                has_qoo10 = True

        lowest = min(prices) if prices else 0
        avg    = round(sum(prices) / len(prices)) if prices else 0

        logger.info(
            f"[카카쿠] '{keyword_jp}' → 최저 ¥{lowest:,}, "
            f"{len(prices)}건, 판매처 {len(sellers)}곳, "
            f"Qoo10포함: {has_qoo10}"
        )

        return {
            "source":       "kakaku",
            "lowest_price": lowest,
            "avg_price":    avg,
            "item_count":   len(prices),
            "sellers":      list(sellers)[:10],
            "has_qoo10":    has_qoo10,
            "top_items":    top_items,
        }

    except Exception as e:
        logger.error(f"[카카쿠] 크롤링 오류 ({keyword_jp}): {e}")
        return _empty_result("kakaku")


# ══════════════════════════════════════════════════════════════
# STEP 3: 큐텐 재팬 크롤링
# ══════════════════════════════════════════════════════════════

def search_qoo10_price(keyword_jp: str) -> dict:
    """
    큐텐 재팬 검색 결과에서 최저가 수집
    Args:
        keyword_jp: 일본어 검색 키워드
    Returns:
        {
            "source": "qoo10",
            "lowest_price": 2400,
            "avg_price": 2700,
            "item_count": 5,
            "top_items": [ ... ]
        }
    """
    delay = random.uniform(QOO10_DELAY_MIN, QOO10_DELAY_MAX)
    time.sleep(delay)

    url = f"https://www.qoo10.jp/s/{quote(keyword_jp)}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)

        if resp.status_code != 200:
            logger.warning(f"[큐텐] HTTP {resp.status_code} for '{keyword_jp}'")
            return _empty_result("qoo10")

        soup = BeautifulSoup(resp.text, "lxml")

        prices = []
        top_items = []

        # 큐텐 검색 결과 파싱
        product_cards = soup.select(
            "div.sbj_item, div[class*='item_wrap'], "
            "li[class*='search_item'], div.g_gallery_item"
        )

        for card in product_cards[:20]:
            # 가격 추출
            price_el = card.select_one(
                "span.prc, strong.prc, span[class*='price'], "
                "strong[class*='price'], em.prc"
            )
            if price_el:
                price = _extract_yen_price(price_el.get_text(strip=True))
                if price > 0:
                    prices.append(price)

            # 상품명
            name_el = card.select_one("a[class*='title'], a[class*='name'], div[class*='sbj'] a")
            item_name = name_el.get_text(strip=True) if name_el else ""
            item_url  = name_el.get("href", "") if name_el else ""

            if item_name and len(top_items) < 5:
                top_items.append({
                    "name":  item_name[:80],
                    "price": prices[-1] if prices else 0,
                    "url":   item_url,
                })

        # 텍스트 폴백
        if not prices:
            all_text = soup.get_text()
            yen_matches = re.findall(r"([\d,]+)\s*円", all_text)
            for m in yen_matches[:10]:
                p = int(m.replace(",", ""))
                if 100 < p < 500000:
                    prices.append(p)

        lowest = min(prices) if prices else 0
        avg    = round(sum(prices) / len(prices)) if prices else 0

        logger.info(f"[큐텐] '{keyword_jp}' → 최저 ¥{lowest:,}, {len(prices)}건")

        return {
            "source":       "qoo10",
            "lowest_price": lowest,
            "avg_price":    avg,
            "item_count":   len(prices),
            "top_items":    top_items,
        }

    except Exception as e:
        logger.error(f"[큐텐] 크롤링 오류 ({keyword_jp}): {e}")
        return _empty_result("qoo10")


# ══════════════════════════════════════════════════════════════
# 통합 3단계 경쟁가 조회
# ══════════════════════════════════════════════════════════════

def get_competitor_prices(keyword_jp: str, skip_qoo10_if_kakaku: bool = True) -> dict:
    """
    3단계 경쟁가 조회 실행
    Args:
        keyword_jp: 일본어 상품 키워드
        skip_qoo10_if_kakaku: 카카쿠에 Qoo10 가격 포함 시 STEP 3 스킵
    Returns:
        {
            "keyword": str,
            "rakuten": { ... },
            "kakaku":  { ... },
            "qoo10":   { ... } or None,
            "overall_lowest": 2300,
            "overall_source": "kakaku",
            "sources_checked": ["rakuten", "kakaku"],
        }
    """
    logger.info(f"[경쟁가] === '{keyword_jp}' 3단계 조회 시작 ===")

    result = {
        "keyword":          keyword_jp,
        "rakuten":          None,
        "kakaku":           None,
        "qoo10":            None,
        "overall_lowest":   0,
        "overall_source":   "",
        "sources_checked":  [],
    }

    # STEP 1: 라쿠텐
    rakuten = search_rakuten_price(keyword_jp)
    result["rakuten"] = rakuten
    result["sources_checked"].append("rakuten")

    # STEP 2: 카카쿠
    kakaku = search_kakaku_price(keyword_jp)
    result["kakaku"] = kakaku
    result["sources_checked"].append("kakaku")

    # STEP 3: 큐텐 (카카쿠에 Qoo10 가격 포함 시 스킵)
    if skip_qoo10_if_kakaku and kakaku.get("has_qoo10"):
        logger.info(f"[경쟁가] 카카쿠에 Qoo10 가격 포함 → STEP 3 스킵")
    else:
        qoo10 = search_qoo10_price(keyword_jp)
        result["qoo10"] = qoo10
        result["sources_checked"].append("qoo10")

    # 종합 최저가 결정
    candidates = []
    for src_name in ["rakuten", "kakaku", "qoo10"]:
        src = result.get(src_name)
        if src and src.get("lowest_price", 0) > 0:
            candidates.append((src_name, src["lowest_price"]))

    if candidates:
        candidates.sort(key=lambda x: x[1])
        result["overall_lowest"] = candidates[0][1]
        result["overall_source"] = candidates[0][0]

    logger.info(
        f"[경쟁가] '{keyword_jp}' 종합 최저가: "
        f"¥{result['overall_lowest']:,} ({result['overall_source']}), "
        f"조회 소스: {result['sources_checked']}"
    )

    return result


def get_competitor_prices_batch(items: list, keyword_field: str = "name_jp") -> list:
    """
    상품 리스트에 대해 일괄 경쟁가 조회
    Args:
        items: 상품 리스트 (name_jp 또는 trend_keyword_jp 필드 필요)
        keyword_field: 검색에 사용할 키워드 필드명
    Returns:
        items에 competitor_info 필드가 추가된 리스트
    """
    for i, item in enumerate(items):
        keyword = item.get(keyword_field, "")
        if not keyword:
            # 폴백: 상품명에서 브랜드+카테고리 추출
            keyword = item.get("trend_keyword_jp", item.get("name", ""))

        if not keyword:
            item["competitor_info"] = _empty_competitor()
            continue

        logger.info(f"[경쟁가 일괄] {i+1}/{len(items)}: '{keyword}'")
        competitor = get_competitor_prices(keyword)
        item["competitor_info"] = competitor
        item["competitor_lowest_jpy"] = competitor.get("overall_lowest", 0)

    return items


# ══════════════════════════════════════════════════════════════
# 유틸리티
# ══════════════════════════════════════════════════════════════

def _extract_yen_price(text: str) -> int:
    """텍스트에서 엔화 가격 추출"""
    text = text.replace(",", "").replace("，", "").replace("￥", "").replace("¥", "")
    text = text.replace("円", "").replace("税込", "").strip()
    m = re.search(r"(\d+)", text)
    if m:
        val = int(m.group(1))
        if 10 < val < 1_000_000:
            return val
    return 0


def _empty_result(source: str) -> dict:
    """빈 결과 템플릿"""
    return {
        "source":       source,
        "lowest_price": 0,
        "avg_price":    0,
        "item_count":   0,
        "top_items":    [],
    }


def _empty_competitor() -> dict:
    """빈 경쟁가 결과"""
    return {
        "keyword":          "",
        "rakuten":          _empty_result("rakuten"),
        "kakaku":           _empty_result("kakaku"),
        "qoo10":            None,
        "overall_lowest":   0,
        "overall_source":   "",
        "sources_checked":  [],
    }


# ══════════════════════════════════════════════════════════════
# 직접 실행 (테스트)
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    test_keywords = [
        "ANUA ドクダミ トナー",
        "TIRTIR クッションファンデ",
        "COSRX スネイル ムチン",
    ]

    for kw in test_keywords:
        print(f"\n{'='*60}")
        result = get_competitor_prices(kw)
        print(f"키워드: {kw}")
        print(f"  라쿠텐: ¥{result['rakuten']['lowest_price']:,} ({result['rakuten']['item_count']}건)")
        print(f"  카카쿠: ¥{result['kakaku']['lowest_price']:,} ({result['kakaku']['item_count']}건)")
        if result['qoo10']:
            print(f"  큐텐:   ¥{result['qoo10']['lowest_price']:,} ({result['qoo10']['item_count']}건)")
        else:
            print(f"  큐텐:   SKIP (카카쿠에 포함)")
        print(f"  → 종합 최저가: ¥{result['overall_lowest']:,} ({result['overall_source']})")
