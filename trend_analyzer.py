"""
trend_analyzer.py – 트렌드 데이터 통합 분석 & 수요 키워드 도출 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.7

PHASE A1(라쿠텐) + A2(구글 트렌드) 결과를 합산하여
최종 수요 키워드 리스트와 소싱 대상 브랜드/카테고리를 결정합니다.
"""

import json
import logging
from datetime import datetime
from collections import Counter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 점수 가중치
# ──────────────────────────────────────────────
WEIGHT_RAKUTEN_RANK = 30       # 라쿠텐 랭킹 순위 기반
WEIGHT_RAKUTEN_REVIEW = 20     # 라쿠텐 리뷰 수/평점
WEIGHT_GOOGLE_INTEREST = 25    # 구글 트렌드 관심도
WEIGHT_GOOGLE_RISING = 25      # 구글 트렌드 급상승


# ──────────────────────────────────────────────
# 라쿠텐 데이터 → 브랜드별 집계
# ──────────────────────────────────────────────
def aggregate_rakuten_brands(rakuten_result):
    """
    라쿠텐 트렌드 결과에서 브랜드별 출현 빈도·평균 순위·리뷰 집계
    Args:
        rakuten_result: trend_rakuten.run_trend_analysis() 반환값
    Returns:
        list of dict sorted by score desc
    """
    brand_data = {}

    korean_items = []
    for label, items in rakuten_result.get("korean_filtered", {}).items():
        korean_items.extend(items)

    if not korean_items:
        logger.warning("한국 코스메 필터링 결과 없음")
        return []

    # 브랜드 키워드 로드
    try:
        with open("korea_brand_keywords.json", "r", encoding="utf-8") as f:
            brand_config = json.load(f)
        brands = brand_config.get("brands", {})
    except FileNotFoundError:
        logger.warning("korea_brand_keywords.json 미발견 – 기본 브랜드 사용")
        brands = _default_brands()

    # 각 상품에 대해 브랜드 매칭
    for item in korean_items:
        name = item.get("item_name", "").lower()
        shop = item.get("shop_name", "").lower()
        text = f"{name} {shop}"

        matched_brand = None
        for brand_name, variants in brands.items():
            for v in variants:
                if v.lower() in text:
                    matched_brand = brand_name
                    break
            if matched_brand:
                break

        if not matched_brand:
            matched_brand = "_기타한국코스메"

        if matched_brand not in brand_data:
            brand_data[matched_brand] = {
                "brand": matched_brand,
                "count": 0,
                "ranks": [],
                "reviews": [],
                "ratings": [],
                "prices": [],
                "sample_items": [],
            }

        bd = brand_data[matched_brand]
        bd["count"] += 1
        if item.get("rank"):
            bd["ranks"].append(item["rank"])
        if item.get("review_count"):
            try:
                bd["reviews"].append(int(item["review_count"]))
            except (ValueError, TypeError):
                pass
        if item.get("review_average"):
            try:
                bd["ratings"].append(float(item["review_average"]))
            except (ValueError, TypeError):
                pass
        if item.get("item_price"):
            try:
                bd["prices"].append(int(item["item_price"]))
            except (ValueError, TypeError):
                pass
        if len(bd["sample_items"]) < 3:
            bd["sample_items"].append({
                "name": item.get("item_name", "")[:60],
                "price": item.get("item_price"),
                "rank": item.get("rank"),
            })

    # 점수 계산
    result = []
    for brand_name, bd in brand_data.items():
        avg_rank = sum(bd["ranks"]) / len(bd["ranks"]) if bd["ranks"] else 999
        total_reviews = sum(bd["reviews"])
        avg_rating = sum(bd["ratings"]) / len(bd["ratings"]) if bd["ratings"] else 0
        avg_price = sum(bd["prices"]) / len(bd["prices"]) if bd["prices"] else 0

        # 랭킹 점수: 순위가 낮을수록 높은 점수 (1위=100, 100위=1)
        rank_score = max(0, (100 - avg_rank) / 100 * WEIGHT_RAKUTEN_RANK)

        # 리뷰 점수: 로그 스케일
        import math
        review_score = min(WEIGHT_RAKUTEN_REVIEW, math.log10(max(total_reviews, 1)) / 5 * WEIGHT_RAKUTEN_REVIEW)

        rakuten_score = round(rank_score + review_score, 1)

        result.append({
            "brand": brand_name,
            "appearance_count": bd["count"],
            "avg_rank": round(avg_rank, 1),
            "total_reviews": total_reviews,
            "avg_rating": round(avg_rating, 2),
            "avg_price_jpy": round(avg_price),
            "rakuten_score": rakuten_score,
            "sample_items": bd["sample_items"],
        })

    result.sort(key=lambda x: x["rakuten_score"], reverse=True)
    logger.info(f"라쿠텐 브랜드 집계: {len(result)}개 브랜드")
    return result


# ──────────────────────────────────────────────
# 구글 트렌드 데이터 → 키워드 점수화
# ──────────────────────────────────────────────
def aggregate_google_trends(google_result):
    """
    구글 트렌드 결과에서 키워드별 관심도·급상승 점수 집계
    Args:
        google_result: trend_google_jp.run_google_trends_analysis() 반환값
    Returns:
        list of dict sorted by score desc
    """
    keyword_scores = {}

    # 1. 관심도 시계열에서 최근 값 추출
    for iot in google_result.get("interest_over_time", []):
        latest = iot.get("latest_scores", {})
        for kw, score in latest.items():
            if kw not in keyword_scores:
                keyword_scores[kw] = {"keyword": kw, "interest": 0, "rising_growth": 0, "sources": []}
            keyword_scores[kw]["interest"] = max(keyword_scores[kw]["interest"], score)
            keyword_scores[kw]["sources"].append("interest_over_time")

    # 2. 급상승 키워드
    for item in google_result.get("rising_keywords", []):
        kw = item.get("keyword", "")
        growth = item.get("growth", 0)
        if kw not in keyword_scores:
            keyword_scores[kw] = {"keyword": kw, "interest": 0, "rising_growth": 0, "sources": []}

        try:
            growth_val = int(str(growth).replace("%", "").replace(",", ""))
        except (ValueError, TypeError):
            growth_val = 9999  # "Breakout"

        keyword_scores[kw]["rising_growth"] = max(keyword_scores[kw]["rising_growth"], growth_val)
        keyword_scores[kw]["sources"].append("rising")

    # 점수 계산
    result = []
    for kw, data in keyword_scores.items():
        interest_score = data["interest"] / 100 * WEIGHT_GOOGLE_INTEREST
        rising_score = min(WEIGHT_GOOGLE_RISING, data["rising_growth"] / 1000 * WEIGHT_GOOGLE_RISING)
        google_score = round(interest_score + rising_score, 1)

        result.append({
            "keyword_jp": kw,
            "interest": data["interest"],
            "rising_growth": data["rising_growth"],
            "google_score": google_score,
            "sources": list(set(data["sources"])),
        })

    result.sort(key=lambda x: x["google_score"], reverse=True)
    logger.info(f"구글 트렌드 키워드 집계: {len(result)}개 키워드")
    return result


# ──────────────────────────────────────────────
# 통합 분석: 최종 소싱 키워드 리스트 생성
# ──────────────────────────────────────────────
def generate_sourcing_keywords(rakuten_brands, google_keywords, top_n=50):
    """
    라쿠텐 브랜드 + 구글 키워드를 합산하여 최종 소싱 대상 키워드 리스트 생성
    Args:
        rakuten_brands: aggregate_rakuten_brands() 반환값
        google_keywords: aggregate_google_trends() 반환값
        top_n: 최종 키워드 수
    Returns:
        list of dict (소싱 키워드 + 점수 + 메타데이터)
    """
    sourcing_list = []

    # 1. 라쿠텐 상위 브랜드 → 소싱 키워드로 변환
    for brand in rakuten_brands:
        if brand["brand"] == "_기타한국코스메":
            continue
        sourcing_list.append({
            "keyword_jp": brand["brand"],
            "keyword_type": "brand",
            "rakuten_score": brand["rakuten_score"],
            "google_score": 0,
            "combined_score": brand["rakuten_score"],
            "appearance_count": brand["appearance_count"],
            "avg_rank": brand["avg_rank"],
            "total_reviews": brand["total_reviews"],
            "avg_price_jpy": brand["avg_price_jpy"],
            "sample_items": brand["sample_items"],
        })

    # 2. 구글 트렌드 키워드 추가
    existing_keywords = {item["keyword_jp"].lower() for item in sourcing_list}
    for gk in google_keywords:
        kw = gk["keyword_jp"]
        if kw.lower() in existing_keywords:
            # 이미 있는 브랜드와 매칭되면 점수 합산
            for item in sourcing_list:
                if item["keyword_jp"].lower() == kw.lower():
                    item["google_score"] = gk["google_score"]
                    item["combined_score"] = round(item["rakuten_score"] + gk["google_score"], 1)
                    break
        else:
            sourcing_list.append({
                "keyword_jp": kw,
                "keyword_type": "keyword",
                "rakuten_score": 0,
                "google_score": gk["google_score"],
                "combined_score": gk["google_score"],
                "interest": gk.get("interest", 0),
                "rising_growth": gk.get("rising_growth", 0),
                "appearance_count": 0,
                "avg_rank": None,
                "total_reviews": 0,
                "avg_price_jpy": 0,
                "sample_items": [],
            })
            existing_keywords.add(kw.lower())

    # 3. 종합 점수 기준 정렬 후 상위 N개
    sourcing_list.sort(key=lambda x: x["combined_score"], reverse=True)
    sourcing_list = sourcing_list[:top_n]

    # 4. 순위 부여
    for i, item in enumerate(sourcing_list):
        item["demand_rank"] = i + 1

    logger.info(f"최종 소싱 키워드: {len(sourcing_list)}개")
    return sourcing_list


# ──────────────────────────────────────────────
# 일본어 → 한국어 역번역 키워드 생성
# ──────────────────────────────────────────────
def generate_korean_search_terms(sourcing_keywords):
    """
    소싱 키워드(일본어)를 KJ9603 검색용 한국어 키워드로 변환
    - 브랜드명: 일본어 → 영문/한국어 매핑 (korea_brand_keywords.json 역방향)
    - 일반 키워드: Google Translate API 또는 수동 매핑
    Returns:
        list of dict with added "keyword_kr" field
    """
    # 브랜드 역매핑 로드
    try:
        with open("korea_brand_keywords.json", "r", encoding="utf-8") as f:
            brand_config = json.load(f)
        brands = brand_config.get("brands", {})
    except FileNotFoundError:
        brands = {}

    # 일본어 → 한국어/영문 역매핑 테이블
    jp_to_kr = {}
    for brand_name, variants in brands.items():
        for v in variants:
            jp_to_kr[v.lower()] = brand_name

    # 일반 카테고리 키워드 매핑
    category_map = {
        "スキンケア": "스킨케어",
        "クレンジング": "클렌징",
        "化粧水": "토너",
        "美容液": "세럼",
        "乳液": "로션",
        "クリーム": "크림",
        "パック": "마스크팩",
        "シートマスク": "시트마스크",
        "日焼け止め": "자외선차단",
        "ファンデーション": "파운데이션",
        "クッションファンデ": "쿠션팩트",
        "リップ": "립",
        "アイシャドウ": "아이섀도",
        "マスカラ": "마스카라",
        "シカ": "시카",
        "CICA": "시카",
        "レチノール": "레티놀",
        "ヒアルロン酸": "히알루론산",
        "ビタミンC": "비타민C",
        "ナイアシンアミド": "나이아신아마이드",
        "トナー": "토너",
        "セラム": "세럼",
        "アンプル": "앰플",
        "エッセンス": "에센스",
        "韓国コスメ": "한국화장품",
        "韓国スキンケア": "한국스킨케어",
        "韓国メイク": "한국메이크업",
        "韓国パック": "한국마스크팩",
        "韓国リップ": "한국립",
        "韓国日焼け止め": "한국선크림",
    }

    for item in sourcing_keywords:
        kw_jp = item["keyword_jp"]

        # 1. 브랜드 역매핑 시도
        kr = jp_to_kr.get(kw_jp.lower())
        if kr:
            item["keyword_kr"] = kr
            item["match_method"] = "brand_reverse"
            continue

        # 2. 카테고리 매핑 시도
        kr = category_map.get(kw_jp)
        if kr:
            item["keyword_kr"] = kr
            item["match_method"] = "category_map"
            continue

        # 3. 영문이면 그대로 사용
        if kw_jp.isascii():
            item["keyword_kr"] = kw_jp
            item["match_method"] = "ascii_passthrough"
            continue

        # 4. 매핑 실패 → 번역 필요 표시 (translator.py에서 처리)
        item["keyword_kr"] = f"[TRANSLATE]{kw_jp}"
        item["match_method"] = "needs_translation"

    translated = sum(1 for item in sourcing_keywords if item.get("match_method") != "needs_translation")
    logger.info(f"한국어 변환: {translated}/{len(sourcing_keywords)}건 완료, "
                f"{len(sourcing_keywords) - translated}건 번역 필요")
    return sourcing_keywords


# ──────────────────────────────────────────────
# 통합 실행
# ──────────────────────────────────────────────
def run_trend_analysis_combined(rakuten_result, google_result):
    """
    PHASE A 통합 분석 실행
    Args:
        rakuten_result: trend_rakuten.run_trend_analysis() 반환값
        google_result: trend_google_jp.run_google_trends_analysis() 반환값
    Returns:
        dict {
            "timestamp": str,
            "rakuten_brands": [...],
            "google_keywords": [...],
            "sourcing_keywords": [...],  ← PHASE B 입력
            "summary": { ... }
        }
    """
    logger.info("========== PHASE A 통합 분석 시작 ==========")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 1. 라쿠텐 브랜드 집계
    rakuten_brands = aggregate_rakuten_brands(rakuten_result)

    # 2. 구글 키워드 집계
    google_keywords = aggregate_google_trends(google_result) if google_result else []

    # 3. 통합 소싱 키워드 생성
    sourcing_keywords = generate_sourcing_keywords(rakuten_brands, google_keywords, top_n=50)

    # 4. 한국어 역번역
    sourcing_keywords = generate_korean_search_terms(sourcing_keywords)

    # 5. 요약
    summary = {
        "timestamp": timestamp,
        "total_rakuten_brands": len(rakuten_brands),
        "total_google_keywords": len(google_keywords),
        "final_sourcing_keywords": len(sourcing_keywords),
        "needs_translation": sum(1 for item in sourcing_keywords if item.get("match_method") == "needs_translation"),
        "top_5_brands": [b["brand"] for b in rakuten_brands[:5]],
        "top_5_keywords": [k["keyword_jp"] for k in google_keywords[:5]],
        "top_10_sourcing": [
            {
                "rank": item["demand_rank"],
                "keyword_jp": item["keyword_jp"],
                "keyword_kr": item.get("keyword_kr", ""),
                "combined_score": item["combined_score"],
            }
            for item in sourcing_keywords[:10]
        ],
    }

    logger.info(f"========== PHASE A 통합 분석 완료 ==========")
    logger.info(f"소싱 키워드: {len(sourcing_keywords)}개, 번역 필요: {summary['needs_translation']}건")
    logger.info(f"TOP 5 브랜드: {summary['top_5_brands']}")
    logger.info(f"TOP 5 키워드: {summary['top_5_keywords']}")

    return {
        "timestamp": timestamp,
        "rakuten_brands": rakuten_brands,
        "google_keywords": google_keywords,
        "sourcing_keywords": sourcing_keywords,
        "summary": summary,
    }


# ──────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────
def _default_brands():
    """korea_brand_keywords.json 미발견 시 기본 브랜드"""
    return {
        "TIRTIR": ["ティルティル", "TIRTIR"],
        "ANUA": ["アヌア", "ANUA"],
        "d'Alba": ["ダルバ", "d'Alba", "dalba"],
        "COSRX": ["COSRX"],
        "MISSHA": ["ミシャ", "MISSHA"],
        "rom&nd": ["ロムアンド", "rom&nd", "romand"],
        "VT": ["VTcosmetic", "VT "],
        "BIOHEAL BOH": ["BIOHEAL", "バイオヒール"],
        "innisfree": ["イニスフリー", "innisfree"],
        "ETUDE": ["エチュード", "ETUDE"],
    }


# ──────────────────────────────────────────────
# 직접 실행 (테스트)
# ──────────────────────────────────────────────
if __name__ == "__main__":
    print("trend_analyzer.py는 단독 실행이 아닌 모듈 호출용입니다.")
    print("사용법:")
    print("  from trend_rakuten import run_trend_analysis")
    print("  from trend_google_jp import run_google_trends_analysis")
    print("  from trend_analyzer import run_trend_analysis_combined")
    print("")
    print("  rakuten = run_trend_analysis()")
    print("  google = run_google_trends_analysis()")
    print("  result = run_trend_analysis_combined(rakuten, google)")
