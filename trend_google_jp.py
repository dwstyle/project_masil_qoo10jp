"""
trend_google_jp.py – Google Trends Japan 기반 한국 뷰티 키워드 트렌드 분석 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.7

pytrends 라이브러리를 사용하여 일본 내 한국 코스메 검색 트렌드를 수집합니다.
- 시드 키워드 관심도 시계열 (interest_over_time)
- 연관 키워드 (related_queries: top + rising)
- 라쿠텐 트렌드에서 추출된 동적 브랜드 키워드 분석
"""

import time
import logging
import random
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Google Trends 429 방지용 딜레이
GT_DELAY_MIN = 5
GT_DELAY_MAX = 12

# 시드 키워드 (일본어, 최대 5개씩 묶어서 호출)
SEED_KEYWORD_GROUPS = [
    ["韓国コスメ", "韓国スキンケア", "韓国メイク", "韓国美容", "Kビューティー"],
    ["TIRTIR", "ANUA", "COSRX", "ダルバ", "ロムアンド"],
    ["韓国パック", "シカクリーム", "韓国日焼け止め", "クッションファンデ", "韓国リップ"],
]


def _safe_delay():
    """Google Trends 429 방지를 위한 랜덤 딜레이"""
    delay = random.uniform(GT_DELAY_MIN, GT_DELAY_MAX)
    logger.info(f"  Google Trends 딜레이: {delay:.1f}초")
    time.sleep(delay)


def fetch_interest_over_time(keyword_group, timeframe="today 3-m"):
    """
    키워드 그룹(최대 5개)의 관심도 시계열 수집
    Args:
        keyword_group: list of str (max 5)
        timeframe: "today 3-m" (3개월), "today 12-m" (1년) 등
    Returns:
        dict { "keywords": [...], "timeframe": str, "data": [...] } or None
    """
    try:
        from pytrends.request import TrendReq
    except ImportError:
        logger.error("pytrends 미설치 – pip install pytrends 필요")
        return None

    try:
        pytrends = TrendReq(hl="ja", tz=540)
        _safe_delay()
        pytrends.build_payload(keyword_group, cat=0, timeframe=timeframe, geo="JP")
        df = pytrends.interest_over_time()

        if df.empty:
            logger.warning(f"관심도 데이터 없음: {keyword_group}")
            return None

        # DataFrame → list of dict 변환
        records = []
        for idx, row in df.iterrows():
            record = {"date": idx.strftime("%Y-%m-%d")}
            for kw in keyword_group:
                if kw in df.columns:
                    record[kw] = int(row[kw])
            records.append(record)

        # 최근 값 기준 정렬 (가장 높은 키워드 확인)
        latest = records[-1] if records else {}
        keyword_scores = {kw: latest.get(kw, 0) for kw in keyword_group}

        logger.info(f"관심도 수집 완료: {keyword_group} → 최근값: {keyword_scores}")
        return {
            "keywords": keyword_group,
            "timeframe": timeframe,
            "data_points": len(records),
            "latest_scores": keyword_scores,
            "data": records,
        }

    except Exception as e:
        logger.error(f"Google Trends 관심도 수집 실패: {e}")
        return None


def fetch_related_queries(keyword):
    """
    단일 키워드의 연관 검색어 수집 (top + rising)
    Returns:
        dict { "keyword": str, "top": [...], "rising": [...] } or None
    """
    try:
        from pytrends.request import TrendReq
    except ImportError:
        logger.error("pytrends 미설치")
        return None

    try:
        pytrends = TrendReq(hl="ja", tz=540)
        _safe_delay()
        pytrends.build_payload([keyword], cat=0, timeframe="today 3-m", geo="JP")
        related = pytrends.related_queries()

        result = {"keyword": keyword, "top": [], "rising": []}

        if keyword in related:
            top_df = related[keyword].get("top")
            rising_df = related[keyword].get("rising")

            if top_df is not None and not top_df.empty:
                result["top"] = top_df.to_dict("records")

            if rising_df is not None and not rising_df.empty:
                result["rising"] = rising_df.to_dict("records")

        logger.info(f"연관 검색어 '{keyword}': top {len(result['top'])}건, rising {len(result['rising'])}건")
        return result

    except Exception as e:
        logger.error(f"연관 검색어 수집 실패 ({keyword}): {e}")
        return None


def fetch_related_topics(keyword):
    """
    단일 키워드의 연관 토픽 수집
    Returns:
        dict { "keyword": str, "top": [...], "rising": [...] } or None
    """
    try:
        from pytrends.request import TrendReq
    except ImportError:
        return None

    try:
        pytrends = TrendReq(hl="ja", tz=540)
        _safe_delay()
        pytrends.build_payload([keyword], cat=0, timeframe="today 3-m", geo="JP")
        topics = pytrends.related_topics()

        result = {"keyword": keyword, "top": [], "rising": []}

        if keyword in topics:
            top_df = topics[keyword].get("top")
            rising_df = topics[keyword].get("rising")

            if top_df is not None and not top_df.empty:
                for _, row in top_df.iterrows():
                    result["top"].append({
                        "title": row.get("topic_title", ""),
                        "type": row.get("topic_type", ""),
                        "value": int(row.get("value", 0)),
                    })

            if rising_df is not None and not rising_df.empty:
                for _, row in rising_df.iterrows():
                    result["rising"].append({
                        "title": row.get("topic_title", ""),
                        "type": row.get("topic_type", ""),
                        "value": str(row.get("value", "")),
                    })

        logger.info(f"연관 토픽 '{keyword}': top {len(result['top'])}건, rising {len(result['rising'])}건")
        return result

    except Exception as e:
        logger.error(f"연관 토픽 수집 실패 ({keyword}): {e}")
        return None


def run_google_trends_analysis(extra_keywords=None):
    """
    PHASE A2: Google Trends JP 분석 실행
    Args:
        extra_keywords: list of str – 라쿠텐 트렌드에서 추출된 동적 키워드
    Returns:
        dict {
            "timestamp": str,
            "interest_over_time": [...],
            "related_queries": [...],
            "rising_keywords": [...],
            "summary": { ... }
        }
    """
    logger.info("========== PHASE A2: Google Trends JP 분석 시작 ==========")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    results = {
        "timestamp": timestamp,
        "interest_over_time": [],
        "related_queries": [],
        "related_topics": [],
        "rising_keywords": [],
        "summary": {},
    }

    # 1. 시드 키워드 그룹별 관심도 수집
    keyword_groups = SEED_KEYWORD_GROUPS.copy()

    # 라쿠텐에서 추출된 동적 키워드 추가 (최대 5개씩 묶기)
    if extra_keywords:
        # 기존 시드와 중복 제거
        existing = set()
        for group in SEED_KEYWORD_GROUPS:
            existing.update(group)
        new_kws = [kw for kw in extra_keywords if kw not in existing]

        # 5개씩 묶기
        for i in range(0, len(new_kws), 5):
            chunk = new_kws[i:i + 5]
            if chunk:
                keyword_groups.append(chunk)
                logger.info(f"동적 키워드 그룹 추가: {chunk}")

    for group in keyword_groups:
        iot = fetch_interest_over_time(group)
        if iot:
            results["interest_over_time"].append(iot)

    # 2. 핵심 키워드 연관 검색어 수집
    core_keywords = ["韓国コスメ", "韓国スキンケア"]
    if extra_keywords:
        core_keywords.extend(extra_keywords[:3])  # 동적 키워드 상위 3개 추가

    for kw in core_keywords:
        rq = fetch_related_queries(kw)
        if rq:
            results["related_queries"].append(rq)
            # rising 키워드 수집
            for item in rq.get("rising", []):
                query = item.get("query", "")
                value = item.get("value", 0)
                if query:
                    results["rising_keywords"].append({
                        "keyword": query,
                        "growth": value,
                        "source_keyword": kw,
                    })

    # 3. 연관 토픽 수집 (메인 키워드만)
    rt = fetch_related_topics("韓国コスメ")
    if rt:
        results["related_topics"].append(rt)

    # 4. 요약 생성
    all_rising = results["rising_keywords"]
    # growth 값이 숫자인 것만 정렬
    numeric_rising = []
    for item in all_rising:
        try:
            item["growth_numeric"] = int(str(item["growth"]).replace("%", "").replace(",", ""))
            numeric_rising.append(item)
        except (ValueError, TypeError):
            # "Breakout" 등 문자열인 경우 최상위로
            item["growth_numeric"] = 999999
            numeric_rising.append(item)

    numeric_rising.sort(key=lambda x: x["growth_numeric"], reverse=True)

    results["summary"] = {
        "timestamp": timestamp,
        "keyword_groups_analyzed": len(keyword_groups),
        "related_queries_collected": len(results["related_queries"]),
        "total_rising_keywords": len(all_rising),
        "top_rising": [
            {"keyword": item["keyword"], "growth": item["growth"]}
            for item in numeric_rising[:15]
        ],
    }

    logger.info(f"========== PHASE A2 완료 ==========")
    logger.info(f"키워드 그룹: {len(keyword_groups)}, 급상승 키워드: {len(all_rising)}건")
    if numeric_rising:
        logger.info(f"TOP 급상승: {[item['keyword'] for item in numeric_rising[:5]]}")

    return results


# ──────────────────────────────────────────────
# 직접 실행 (테스트용)
# ──────────────────────────────────────────────
if __name__ == "__main__":
    result = run_google_trends_analysis()

    print("\n" + "=" * 60)
    print(f"수집 시각: {result['timestamp']}")
    print(f"키워드 그룹 분석: {result['summary']['keyword_groups_analyzed']}개")
    print(f"급상승 키워드: {result['summary']['total_rising_keywords']}건")
    print(f"\nTOP 급상승 키워드:")
    for item in result["summary"].get("top_rising", [])[:15]:
        print(f"  {item['keyword']}: +{item['growth']}")
