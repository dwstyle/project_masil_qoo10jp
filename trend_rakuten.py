"""
trend_rakuten.py – 라쿠텐 이치바 랭킹 & 장르 API 기반 일본 뷰티 트렌드 수집 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.7
"""

import os
import time
import json
import logging
import requests
from datetime import datetime

# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────
RAKUTEN_APP_ID = os.environ.get("RAKUTEN_APP_ID", "")
RAKUTEN_ACCESS_KEY = os.environ.get("RAKUTEN_ACCESS_KEY", "")

RANKING_ENDPOINT = "https://openapi.rakuten.co.jp/ichibaranking/api/IchibaItem/Ranking/20220601"
GENRE_SEARCH_ENDPOINT = "https://openapi.rakuten.co.jp/ichibagt/api/IchibaGenre/Search/20170711"
ITEM_SEARCH_ENDPOINT = "https://openapi.rakuten.co.jp/ichiba/api/IchibaItem/Search/20220601"

# 라쿠텐 뷰티 장르 ID (수동 확인 + 동적 보충)
BEAUTY_GENRE_ROOT = 100939          # 美容・コスメ・香水
KNOWN_BEAUTY_GENRES = {
    100939: "美容・コスメ・香水",
    100944: "スキンケア",
    564517: "韓国コスメ",
    111120: "香水・フレグランス",
}

# 한국 뷰티 브랜드 키워드 (일본어 표기)
KOREA_KEYWORDS = [
    "韓国", "韓国コスメ", "Korea", "Korean",
    # 주요 브랜드
    "ティルティル", "TIRTIR",
    "アヌア", "ANUA",
    "ダルバ", "d'Alba", "dalba",
    "COSRX", "コスアールエックス",
    "ミシャ", "MISSHA",
    "イニスフリー", "innisfree",
    "エチュード", "ETUDE",
    "クリオ", "CLIO",
    "ペリペラ", "PERIPERA",
    "ロムアンド", "rom&nd", "romand",
    "ヒンス", "hince",
    "ラネージュ", "LANEIGE",
    "トリデン", "Torriden",
    "メディキューブ", "medicube",
    "VT", "ブイティー",
    "BIOHEAL BOH", "バイオヒールボ",
    "アモーレパシフィック", "Amorepacific",
    "ネイチャーリパブリック", "NATURE REPUBLIC",
    "スキンフード", "SKINFOOD",
    "バニラコ", "banila co",
    "ソンアンド", "Son&Park",
    "ジョンセンムル", "JUNG SAEM MOOL",
    "アミューズ", "AMUSE",
    "ウォンジョンヨ", "Wonjungyo",
    "クンダル", "KUNDAL",
    "マジックフォレスト",
    "ドクタージー", "Dr.G",
    "シカ", "CICA",
    "ビーグレン",
]

# API 호출 제한 (초당 1회)
API_CALL_DELAY = 1.1

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────
def _api_get(url, params):
    """라쿠텐 API GET 요청 (레이트 리밋 준수)"""
    params["applicationId"] = RAKUTEN_APP_ID
    if RAKUTEN_ACCESS_KEY:
        params["accessKey"] = RAKUTEN_ACCESS_KEY
    params.setdefault("formatVersion", 2)

    time.sleep(API_CALL_DELAY)
    try:
        resp = requests.get(url, params=params, timeout=15)
        if resp.status_code == 429:
            logger.warning("429 Too Many Requests – 30초 대기 후 재시도")
            time.sleep(30)
            resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error(f"API 요청 실패: {e}")
        return None


def _is_korean_product(item_name, shop_name=""):
    """상품명 또는 샵명에 한국 뷰티 키워드가 포함되어 있는지 확인"""
    text = f"{item_name} {shop_name}".lower()
    for kw in KOREA_KEYWORDS:
        if kw.lower() in text:
            return True
    return False


# ──────────────────────────────────────────────
# 장르 탐색
# ──────────────────────────────────────────────
def fetch_beauty_sub_genres(parent_genre_id=BEAUTY_GENRE_ROOT):
    """
    라쿠텐 장르검색 API로 뷰티 하위 장르 ID·이름 수집
    Returns: dict {genreId: genreName, ...}
    """
    logger.info(f"장르 탐색 시작: parentGenreId={parent_genre_id}")
    data = _api_get(GENRE_SEARCH_ENDPOINT, {"genreId": parent_genre_id})
    if not data:
        logger.warning("장르 API 응답 없음 – KNOWN_BEAUTY_GENRES 사용")
        return KNOWN_BEAUTY_GENRES.copy()

    genres = {}
    for child in data.get("children", []):
        gid = child.get("genreId")
        gname = child.get("genreName", "")
        if gid:
            genres[gid] = gname
    logger.info(f"하위 장르 {len(genres)}개 발견: {list(genres.values())}")
    return genres


# ──────────────────────────────────────────────
# 랭킹 수집
# ──────────────────────────────────────────────
def fetch_ranking(genre_id=None, sex=None, age=None, period="realtime", pages=3):
    """
    라쿠텐 랭킹 API 호출.
    - genre_id: 장르ID (sex/age와 동시 사용 불가)
    - sex: 0=남, 1=여
    - age: 10,20,30,40,50
    - period: realtime (default)
    - pages: 수집 페이지 수 (1페이지=30건, max 34페이지)
    Returns: list of dict
    """
    items = []
    for page in range(1, pages + 1):
        params = {"page": page}
        if genre_id and not sex and not age:
            params["genreId"] = genre_id
        if sex is not None:
            params["sex"] = sex
        if age is not None:
            params["age"] = age

        data = _api_get(RANKING_ENDPOINT, params)
        if not data:
            break

        page_items = data.get("Items", [])
        if not page_items:
            break

        for item in page_items:
            items.append({
                "rank": item.get("rank"),
                "item_name": item.get("itemName", ""),
                "item_price": item.get("itemPrice"),
                "item_url": item.get("itemUrl", ""),
                "shop_name": item.get("shopName", ""),
                "shop_url": item.get("shopUrl", ""),
                "genre_id": item.get("genreId"),
                "review_count": item.get("reviewCount", 0),
                "review_average": item.get("reviewAverage", 0),
                "image_url": (item.get("mediumImageUrls") or [""])[0] if item.get("mediumImageUrls") else "",
                "availability": item.get("availability", 1),
            })

        logger.info(f"  page {page}: {len(page_items)}건 수집")

    return items


def fetch_beauty_rankings():
    """
    뷰티 전체 + 주요 하위 장르 + 성별/연령별 랭킹 수집
    Returns: dict { "query_label": [items], ... }
    """
    results = {}

    # ① 뷰티 전체 (상위 90건)
    logger.info("=== 뷰티 전체 랭킹 ===")
    results["beauty_overall"] = fetch_ranking(genre_id=BEAUTY_GENRE_ROOT, pages=3)

    # ② 한국코스메 장르 (564517)
    logger.info("=== 韓国コスメ 장르 랭킹 ===")
    results["korean_cosme"] = fetch_ranking(genre_id=564517, pages=3)

    # ③ 스킨케어 장르
    logger.info("=== スキンケア 장르 랭킹 ===")
    results["skincare"] = fetch_ranking(genre_id=100944, pages=2)

    # ④ 여성 20대 뷰티 (genreId 사용 불가 → sex+age만)
    logger.info("=== 女性20代 랭킹 ===")
    results["female_20s"] = fetch_ranking(sex=1, age=20, pages=2)

    # ⑤ 여성 30대 뷰티
    logger.info("=== 女性30代 랭킹 ===")
    results["female_30s"] = fetch_ranking(sex=1, age=30, pages=2)

    return results


# ──────────────────────────────────────────────
# 한국 코스메 필터링
# ──────────────────────────────────────────────
def filter_korean_products(items):
    """한국 뷰티 키워드가 포함된 상품만 필터링"""
    filtered = []
    for item in items:
        if _is_korean_product(item["item_name"], item.get("shop_name", "")):
            item["is_korean"] = True
            filtered.append(item)
    logger.info(f"한국 코스메 필터: {len(items)}건 → {len(filtered)}건")
    return filtered


# ──────────────────────────────────────────────
# 키워드 추출
# ──────────────────────────────────────────────
def extract_trending_keywords(items, top_n=30):
    """
    랭킹 상품명에서 자주 등장하는 키워드 추출 (간이 버전)
    Returns: list of (keyword, count)
    """
    from collections import Counter

    # 불용어
    stopwords = {
        "の", "・", "【", "】", "＜", "＞", "（", "）", "(", ")", "/", "｜",
        "公式", "送料無料", "ポイント", "倍", "クーポン", "OFF", "限定",
        "セット", "お得", "まとめ買い", "楽天", "ランキング", "1位", "入り",
        "新品", "正規品", "日本", "円", "税込", "ml", "g", "kg", "本",
        "個", "枚", "セール", "特価", "お試し", "サンプル",
    }

    word_counter = Counter()
    for item in items:
        name = item.get("item_name", "")
        # 간이 토큰화: 스페이스 + 기호 분리
        tokens = name.replace("【", " ").replace("】", " ").replace("（", " ").replace("）", " ")
        tokens = tokens.replace("/", " ").replace("｜", " ").replace("・", " ")
        for token in tokens.split():
            token = token.strip()
            if len(token) >= 2 and token not in stopwords:
                word_counter[token] += 1

    return word_counter.most_common(top_n)


# ──────────────────────────────────────────────
# 라쿠텐 아이템 검색 (키워드 역검색용)
# ──────────────────────────────────────────────
def search_items_by_keyword(keyword, genre_id=BEAUTY_GENRE_ROOT, pages=1):
    """
    라쿠텐 상품검색 API로 키워드 검색
    Returns: list of dict (item info)
    """
    items = []
    for page in range(1, pages + 1):
        params = {
            "keyword": keyword,
            "genreId": genre_id,
            "page": page,
            "hits": 30,
            "sort": "-reviewCount",  # 리뷰 많은 순
        }
        data = _api_get(ITEM_SEARCH_ENDPOINT, params)
        if not data:
            break

        for item in data.get("Items", []):
            items.append({
                "item_name": item.get("itemName", ""),
                "item_price": item.get("itemPrice"),
                "item_url": item.get("itemUrl", ""),
                "shop_name": item.get("shopName", ""),
                "review_count": item.get("reviewCount", 0),
                "review_average": item.get("reviewAverage", 0),
                "image_url": (item.get("mediumImageUrls") or [""])[0] if item.get("mediumImageUrls") else "",
                "genre_id": item.get("genreId"),
            })
    logger.info(f"키워드 '{keyword}' 검색: {len(items)}건")
    return items


# ──────────────────────────────────────────────
# 메인 실행 (트렌드 분석 리포트 생성)
# ──────────────────────────────────────────────
def run_trend_analysis():
    """
    PHASE A1: 라쿠텐 트렌드 분석 실행
    Returns: dict {
        "timestamp": str,
        "rankings": { label: [items] },
        "korean_filtered": { label: [items] },
        "trending_keywords": [(keyword, count)],
        "summary": { ... }
    }
    """
    logger.info("========== PHASE A1: 라쿠텐 트렌드 분석 시작 ==========")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 1. 장르 탐색
    sub_genres = fetch_beauty_sub_genres()

    # 2. 랭킹 수집
    rankings = fetch_beauty_rankings()

    # 3. 한국 코스메 필터링
    korean_filtered = {}
    total_korean = 0
    for label, items in rankings.items():
        korean = filter_korean_products(items)
        if korean:
            korean_filtered[label] = korean
            total_korean += len(korean)

    # 4. 트렌드 키워드 추출 (한국 코스메 전체에서)
    all_korean_items = []
    for items in korean_filtered.values():
        all_korean_items.extend(items)
    trending_keywords = extract_trending_keywords(all_korean_items, top_n=30)

    # 5. 요약 생성
    total_items = sum(len(v) for v in rankings.values())
    summary = {
        "timestamp": timestamp,
        "total_items_scanned": total_items,
        "total_korean_items": total_korean,
        "korean_ratio": round(total_korean / max(total_items, 1) * 100, 1),
        "sub_genres_found": len(sub_genres),
        "top_keywords": [kw for kw, cnt in trending_keywords[:10]],
        "top_korean_brands": _extract_top_brands(all_korean_items),
    }

    logger.info(f"========== PHASE A1 완료 ==========")
    logger.info(f"총 스캔: {total_items}건, 한국 코스메: {total_korean}건 ({summary['korean_ratio']}%)")
    logger.info(f"상위 키워드: {summary['top_keywords']}")

    return {
        "timestamp": timestamp,
        "sub_genres": sub_genres,
        "rankings": rankings,
        "korean_filtered": korean_filtered,
        "trending_keywords": trending_keywords,
        "summary": summary,
    }


def _extract_top_brands(items, top_n=10):
    """한국 코스메 상품에서 브랜드 빈도 추출"""
    from collections import Counter

    brand_keywords = {
        "TIRTIR": ["ティルティル", "TIRTIR"],
        "ANUA": ["アヌア", "ANUA"],
        "d'Alba": ["ダルバ", "d'Alba", "dalba"],
        "COSRX": ["COSRX", "コスアールエックス"],
        "MISSHA": ["ミシャ", "MISSHA"],
        "innisfree": ["イニスフリー", "innisfree"],
        "ETUDE": ["エチュード", "ETUDE"],
        "CLIO": ["クリオ", "CLIO"],
        "PERIPERA": ["ペリペラ", "PERIPERA"],
        "rom&nd": ["ロムアンド", "rom&nd", "romand"],
        "hince": ["ヒンス", "hince"],
        "Torriden": ["トリデン", "Torriden"],
        "medicube": ["メディキューブ", "medicube"],
        "VT": ["VTcosmetic", "VT "],
        "BIOHEAL BOH": ["BIOHEAL", "バイオヒール"],
        "LANEIGE": ["ラネージュ", "LANEIGE"],
        "AMUSE": ["アミューズ", "AMUSE"],
        "Wonjungyo": ["ウォンジョンヨ", "Wonjungyo"],
        "KUNDAL": ["クンダル", "KUNDAL"],
        "Dr.G": ["ドクタージー", "Dr.G"],
    }

    counter = Counter()
    for item in items:
        name = item.get("item_name", "")
        for brand, variants in brand_keywords.items():
            for v in variants:
                if v.lower() in name.lower():
                    counter[brand] += 1
                    break

    return [brand for brand, _ in counter.most_common(top_n)]


# ──────────────────────────────────────────────
# 직접 실행 (테스트용)
# ──────────────────────────────────────────────
if __name__ == "__main__":
    if not RAKUTEN_APP_ID:
        print("ERROR: RAKUTEN_APP_ID 환경변수를 설정해주세요.")
        print("예: export RAKUTEN_APP_ID=your_app_id")
        exit(1)

    result = run_trend_analysis()

    # 결과 출력
    print("\n" + "=" * 60)
    print(f"수집 시각: {result['timestamp']}")
    print(f"총 스캔: {result['summary']['total_items_scanned']}건")
    print(f"한국 코스메: {result['summary']['total_korean_items']}건 ({result['summary']['korean_ratio']}%)")
    print(f"\n상위 브랜드: {result['summary']['top_korean_brands']}")
    print(f"\n트렌드 키워드 TOP 20:")
    for kw, cnt in result["trending_keywords"][:20]:
        print(f"  {kw}: {cnt}회")
