"""
main_qoo10.py – Qoo10 Japan 뷰티 소싱 자동화 메인 파이프라인
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.7

파이프라인 흐름:
  PHASE A: 일본 뷰티 트렌드 분석
    A1. 라쿠텐 랭킹 API → 한국 코스메 필터링
    A2. Google Trends JP → 급상승 키워드 수집
    A3. 통합 분석 → 소싱 키워드 리스트 생성

  PHASE B: KJ9603 상품 매칭
    B1. 트렌드 키워드 → KJ9603 검색
    B2. PASS 1 필터링 (가격 범위)

  PHASE C: 상세 분석 & 가격 계산
    C1. 상세 페이지 크롤링 (옵션, 배송비, 이미지)
    C2. 원가 계산 → 판매가 역산
    C3. 경쟁가 3단계 조회 (라쿠텐→카카쿠→큐텐)
    C4. 스코어링 (100점 만점)
    C5. 최종 필터 (≥70점 + 마진≥20% + 가격경쟁력)

  PHASE D: 출력
    D1. 상품명 일본어 번역
    D2. 상세페이지 HTML 생성
    D3. Google Sheets 업데이트
    D4. Qoo10 엑셀 생성 → Drive 업로드
"""

import logging
import time
from datetime import datetime

# ── 모듈 임포트 ───────────────────────────────────────────────
from trend_rakuten import run_trend_analysis
from trend_google_jp import run_google_trends_analysis
from trend_analyzer import run_trend_analysis_combined
from crawler_kj import (
    get_session, close_driver,
    search_by_trend_keywords, fetch_item_detail,
    pass1_filter,
)
from price_calculator import calculate_prices_batch
from competitor_price import get_competitor_prices
from scorer import calculate_scores_batch, get_final_candidates
from translator import translate_items_batch, generate_detail_html_batch
from sheet_updater import update_all_sheets
from uploader_qoo10 import generate_and_upload

# ── 설정 ──────────────────────────────────────────────────────
PASS2_LIMIT = 150          # PHASE C 상세 크롤링 최대 건수
LOG_FORMAT  = "%(asctime)s [%(levelname)s] %(message)s"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# 메인 파이프라인
# ══════════════════════════════════════════════════════════════

def main():
    start_time = time.time()
    timestamp  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"{'='*60}")
    logger.info(f"Plan B Cabinet – Qoo10 JP Beauty Sourcing Pipeline")
    logger.info(f"시작: {timestamp}")
    logger.info(f"{'='*60}")

    run_summary = {
        "run_timestamp": timestamp,
        "status": "started",
    }

    driver = None

    try:
        # ══════════════════════════════════════════════
        # PHASE A: 일본 뷰티 트렌드 분석
        # ══════════════════════════════════════════════
        logger.info("\n" + "=" * 50)
        logger.info("PHASE A: 일본 뷰티 트렌드 분석")
        logger.info("=" * 50)

        # A1: 라쿠텐 랭킹
        logger.info("\n--- A1: 라쿠텐 랭킹 수집 ---")
        rakuten_result = run_trend_analysis()
        run_summary["a1_rakuten_items"] = rakuten_result["summary"]["total_items_scanned"]
        run_summary["a1_korean_items"]  = rakuten_result["summary"]["total_korean_items"]

        # A2: Google Trends JP
        logger.info("\n--- A2: Google Trends JP 분석 ---")
        try:
            extra_keywords = rakuten_result["summary"].get("top_keywords", [])[:5]
            google_result = run_google_trends_analysis(extra_keywords=extra_keywords)
            run_summary["a2_rising_keywords"] = google_result["summary"]["total_rising_keywords"]
        except Exception as e:
            logger.warning(f"Google Trends 실패 (계속 진행): {e}")
            google_result = None
            run_summary["a2_rising_keywords"] = 0

        # A3: 통합 분석
        logger.info("\n--- A3: 통합 트렌드 분석 ---")
        trend_combined = run_trend_analysis_combined(rakuten_result, google_result)
        sourcing_keywords = trend_combined.get("sourcing_keywords", [])
        run_summary["a3_sourcing_keywords"] = len(sourcing_keywords)

        logger.info(f"PHASE A 완료: 소싱 키워드 {len(sourcing_keywords)}개")

        # ══════════════════════════════════════════════
        # PHASE B: KJ9603 상품 매칭
        # ══════════════════════════════════════════════
        logger.info("\n" + "=" * 50)
        logger.info("PHASE B: KJ9603 상품 매칭")
        logger.info("=" * 50)

        # B1: KJ9603 로그인 & 검색
        logger.info("\n--- B1: KJ9603 로그인 & 키워드 검색 ---")
        driver = get_session()
        raw_items = search_by_trend_keywords(driver, sourcing_keywords, max_per_keyword=2)
        run_summary["b1_raw_items"] = len(raw_items)

        # B2: PASS 1 필터
        logger.info("\n--- B2: PASS 1 필터링 ---")
        pass1_items = pass1_filter(raw_items)
        run_summary["b2_pass1_items"] = len(pass1_items)

        logger.info(f"PHASE B 완료: {len(raw_items)}건 → PASS1 {len(pass1_items)}건")

        if not pass1_items:
            logger.warning("PASS 1 통과 상품 없음 – 파이프라인 종료")
            run_summary["status"] = "no_items"
            _finish(run_summary, trend_combined, [], [], start_time)
            return

        # ══════════════════════════════════════════════
        # PHASE C: 상세 분석 & 가격 계산
        # ══════════════════════════════════════════════
        logger.info("\n" + "=" * 50)
        logger.info("PHASE C: 상세 분석 & 가격 계산")
        logger.info("=" * 50)

        # PASS2 제한 적용
        pass2_items = pass1_items[:PASS2_LIMIT]
        logger.info(f"PASS 2 대상: {len(pass2_items)}건 (LIMIT: {PASS2_LIMIT})")

        # C1: 상세 페이지 크롤링
        logger.info("\n--- C1: 상세 페이지 크롤링 ---")
        for i, item in enumerate(pass2_items):
            logger.info(f"  [{i+1}/{len(pass2_items)}] {item.get('name', '')[:40]}")
            try:
                detail = fetch_item_detail(driver, item["url"])
                item["detail_info"]    = detail
                item["kj_shipping"]    = detail.get("shipping_fee", 3500)
                item["detail_images"]  = detail.get("detail_images", [])
                item["has_option"]     = detail.get("has_option", False)
            except Exception as e:
                logger.warning(f"  상세 크롤링 실패: {e}")
                item["detail_info"]   = {}
                item["detail_images"] = []
            time.sleep(0.5)

        run_summary["c1_detail_crawled"] = len(pass2_items)

        # C2: 원가 계산
        logger.info("\n--- C2: 원가 계산 ---")
        priced_items = calculate_prices_batch(pass2_items)
        profitable = [item for item in priced_items if item.get("is_profitable")]
        run_summary["c2_profitable"] = len(profitable)

        logger.info(f"수익성 충족: {len(profitable)}/{len(priced_items)}건")

        if not profitable:
            logger.warning("수익성 충족 상품 없음 – 파이프라인 종료")
            run_summary["status"] = "no_profitable"
            _finish(run_summary, trend_combined, priced_items, [], start_time)
            if driver:
                close_driver(driver)
            return

        # C3: 경쟁가 3단계 조회
        logger.info("\n--- C3: 경쟁가 3단계 조회 ---")
        for i, item in enumerate(profitable):
            keyword_jp = item.get("name_jp", "") or item.get("trend_keyword_jp", "") or item.get("name", "")
            logger.info(f"  [{i+1}/{len(profitable)}] {keyword_jp[:40]}")
            try:
                competitor = get_competitor_prices(keyword_jp)
                item["competitor_info"]       = competitor
                item["competitor_lowest_jpy"] = competitor.get("overall_lowest", 0)
            except Exception as e:
                logger.warning(f"  경쟁가 조회 실패: {e}")
                item["competitor_info"]       = {}
                item["competitor_lowest_jpy"] = 0

        run_summary["c3_competitor_checked"] = len(profitable)

        # WebDriver 종료 (더 이상 불필요)
        close_driver(driver)
        driver = None

        # C4: 스코어링
        logger.info("\n--- C4: 스코어링 ---")
        scored_items = calculate_scores_batch(profitable)

        # C5: 최종 필터
        logger.info("\n--- C5: 최종 필터 ---")
        final_candidates = get_final_candidates(scored_items)
        run_summary["c4_scored"]    = len(scored_items)
        run_summary["c5_final"]     = len(final_candidates)

        logger.info(f"PHASE C 완료: 최종 후보 {len(final_candidates)}건")

        # ══════════════════════════════════════════════
        # PHASE D: 출력
        # ══════════════════════════════════════════════
        logger.info("\n" + "=" * 50)
        logger.info("PHASE D: 출력 (번역·HTML·시트·엑셀)")
        logger.info("=" * 50)

        if final_candidates:
            # D1: 일본어 번역
            logger.info("\n--- D1: 상품명 번역 ---")
            final_candidates = translate_items_batch(final_candidates)

            # D2: 상세페이지 HTML 생성
            logger.info("\n--- D2: 상세페이지 HTML 생성 ---")
            final_candidates = generate_detail_html_batch(final_candidates)

            # D3: Google Sheets 업데이트
            logger.info("\n--- D3: Google Sheets 업데이트 ---")
            try:
                update_all_sheets(trend_combined, scored_items, final_candidates, run_summary)
            except Exception as e:
                logger.error(f"Sheets 업데이트 실패: {e}")

            # D4: 엑셀 생성 & Drive 업로드
            logger.info("\n--- D4: 엑셀 생성 & Drive 업로드 ---")
            try:
                upload_result = generate_and_upload(final_candidates)
                run_summary["d4_upload_url"]   = upload_result.get("drive_url", "")
                run_summary["d4_upload_count"] = upload_result.get("item_count", 0)
            except Exception as e:
                logger.error(f"엑셀/Drive 업로드 실패: {e}")
        else:
            logger.info("최종 후보 0건 – 출력 단계 스킵")

        run_summary["status"] = "completed"
        _finish(run_summary, trend_combined, scored_items, final_candidates, start_time)

    except Exception as e:
        logger.error(f"파이프라인 오류: {e}", exc_info=True)
        run_summary["status"] = f"error: {str(e)}"
    finally:
        if driver:
            close_driver(driver)


# ══════════════════════════════════════════════════════════════
# 완료 리포트
# ══════════════════════════════════════════════════════════════

def _finish(run_summary, trend_data, scored_items, final_candidates, start_time):
    """파이프라인 완료 리포트 출력"""
    elapsed = round(time.time() - start_time, 1)
    run_summary["elapsed_seconds"] = elapsed
    run_summary["elapsed_minutes"] = round(elapsed / 60, 1)

    logger.info(f"\n{'='*60}")
    logger.info(f"파이프라인 완료 리포트")
    logger.info(f"{'='*60}")
    logger.info(f"상태: {run_summary.get('status', 'unknown')}")
    logger.info(f"소요 시간: {run_summary['elapsed_minutes']}분 ({elapsed}초)")
    logger.info(f"")
    logger.info(f"[PHASE A] 트렌드 분석")
    logger.info(f"  라쿠텐 스캔: {run_summary.get('a1_rakuten_items', 0)}건")
    logger.info(f"  한국 코스메: {run_summary.get('a1_korean_items', 0)}건")
    logger.info(f"  급상승 키워드: {run_summary.get('a2_rising_keywords', 0)}건")
    logger.info(f"  소싱 키워드: {run_summary.get('a3_sourcing_keywords', 0)}개")
    logger.info(f"")
    logger.info(f"[PHASE B] KJ9603 매칭")
    logger.info(f"  검색 결과: {run_summary.get('b1_raw_items', 0)}건")
    logger.info(f"  PASS 1 통과: {run_summary.get('b2_pass1_items', 0)}건")
    logger.info(f"")
    logger.info(f"[PHASE C] 상세 분석")
    logger.info(f"  상세 크롤링: {run_summary.get('c1_detail_crawled', 0)}건")
    logger.info(f"  수익성 충족: {run_summary.get('c2_profitable', 0)}건")
    logger.info(f"  경쟁가 조회: {run_summary.get('c3_competitor_checked', 0)}건")
    logger.info(f"  스코어링: {run_summary.get('c4_scored', 0)}건")
    logger.info(f"  최종 후보: {run_summary.get('c5_final', 0)}건")
    logger.info(f"")
    logger.info(f"[PHASE D] 출력")
    logger.info(f"  업로드: {run_summary.get('d4_upload_count', 0)}건")
    logger.info(f"  Drive URL: {run_summary.get('d4_upload_url', 'N/A')}")
    logger.info(f"{'='*60}")

    # 최종 후보 상위 10개 요약
    if final_candidates:
        logger.info(f"\n최종 후보 TOP 10:")
        for i, item in enumerate(final_candidates[:10]):
            logger.info(
                f"  {i+1}. [{item.get('grade','?')}] "
                f"{item.get('name_jp', item.get('name', ''))[:40]} "
                f"¥{item.get('sell_price_jpy', 0):,} "
                f"(마진 {item.get('margin_rate', 0)*100:.1f}%, "
                f"점수 {item.get('final_score', 0)})"
            )


# ══════════════════════════════════════════════════════════════
# 실행
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    main()
