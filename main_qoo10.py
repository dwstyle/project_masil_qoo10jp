"""
main_qoo10.py – Qoo10 Japan Beauty Sourcing Pipeline Orchestrator
v0.8 – 2026-03-04
crawler_kj v0.9 통합, run_phase_b() 호출, filter_pass1 반영
"""

import os
import sys
import time
import json
import logging
from datetime import datetime

# ═══════════════════════════════════════════════
# 로깅 설정
# ═══════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════
# 모듈 임포트
# ═══════════════════════════════════════════════
try:
    from trend_rakuten import run_trend_analysis as run_rakuten_trend
except ImportError as e:
    logger.warning(f"trend_rakuten import 실패: {e}")
    run_rakuten_trend = None

try:
    from trend_google_jp import run_google_trends_analysis
except ImportError as e:
    logger.warning(f"trend_google_jp import 실패: {e}")
    run_google_trends_analysis = None

try:
    from trend_analyzer import run_trend_analysis_combined
except ImportError as e:
    logger.warning(f"trend_analyzer import 실패: {e}")
    run_trend_analysis_combined = None

try:
    from crawler_kj import (
        get_session,
        close_driver,
        run_phase_b,
        filter_pass1,
        fetch_item_detail,
        fetch_items_detail_batch,
    )
except ImportError as e:
    logger.error(f"crawler_kj import 실패: {e}")
    sys.exit(1)

try:
    from price_calculator import calculate_price, calculate_prices_batch
except ImportError as e:
    logger.warning(f"price_calculator import 실패: {e}")
    calculate_prices_batch = None

try:
    from competitor_price import get_competitor_prices, get_competitor_prices_batch
except ImportError as e:
    logger.warning(f"competitor_price import 실패: {e}")
    get_competitor_prices_batch = None

try:
    from scorer import calculate_scores_batch, get_final_candidates
except ImportError as e:
    logger.warning(f"scorer import 실패: {e}")
    calculate_scores_batch = None
    get_final_candidates = None

try:
    from translator import translate_items_batch, generate_detail_html_batch
except ImportError as e:
    logger.warning(f"translator import 실패: {e}")
    translate_items_batch = None
    generate_detail_html_batch = None

try:
    from sheet_updater import update_all_sheets
except ImportError as e:
    logger.warning(f"sheet_updater import 실패: {e}")
    update_all_sheets = None

try:
    from uploader_qoo10 import generate_and_upload
except ImportError as e:
    logger.warning(f"uploader_qoo10 import 실패: {e}")
    generate_and_upload = None


# ═══════════════════════════════════════════════
# 상수
# ═══════════════════════════════════════════════
PASS2_LIMIT = 150  # 상세 크롤링 최대 건수


# ═══════════════════════════════════════════════
# PHASE A: 트렌드 분석
# ═══════════════════════════════════════════════
def phase_a():
    """PHASE A: 일본 뷰티 트렌드 분석"""
    logger.info("=" * 60)
    logger.info("PHASE A: 일본 뷰티 트렌드 분석 시작")
    logger.info("=" * 60)

    rakuten_result = None
    google_result = None
    combined_result = None

    # ── A1: Rakuten 랭킹 수집 ──
    if run_rakuten_trend:
        try:
            logger.info("── PHASE A1: Rakuten 뷰티 랭킹 수집 ──")
            rakuten_result = run_rakuten_trend()
            summary = rakuten_result.get("summary", {})
            logger.info(
                f"Rakuten 완료: 스캔 {summary.get('total_items_scanned', 0)}건, "
                f"한국코스메 {summary.get('korean_items', 0)}건 "
                f"({summary.get('korean_ratio', 0)}%)"
            )
        except Exception as e:
            logger.error(f"PHASE A1 오류: {e}")
    else:
        logger.warning("PHASE A1 스킵: trend_rakuten 모듈 없음")

    # ── A2: Google Trends JP 수집 ──
    if run_google_trends_analysis:
        try:
            logger.info("── PHASE A2: Google Trends JP 수집 ──")
            # Rakuten에서 추출한 동적 키워드 추가
            extra_kw = []
            if rakuten_result:
                extra_kw = rakuten_result.get("summary", {}).get("top_keywords", [])[:5]
            google_result = run_google_trends_analysis(extra_keywords=extra_kw if extra_kw else None)
            logger.info(f"Google Trends 완료: {google_result.get('summary', {})}")
        except Exception as e:
            logger.error(f"PHASE A2 오류: {e}")
    else:
        logger.warning("PHASE A2 스킵: trend_google_jp 모듈 없음")

    # ── A3: 통합 분석 ──
    if run_trend_analysis_combined and (rakuten_result or google_result):
        try:
            logger.info("── PHASE A3: 통합 트렌드 분석 ──")
            combined_result = run_trend_analysis_combined(rakuten_result, google_result)
            summary = combined_result.get("summary", {})
            logger.info(
                f"통합 분석 완료: 소싱 키워드 {summary.get('final_sourcing_keywords', 0)}개, "
                f"브랜드 {summary.get('total_rakuten_brands', 0)}개"
            )
        except Exception as e:
            logger.error(f"PHASE A3 오류: {e}")
    else:
        logger.warning("PHASE A3 스킵: 데이터 부족 또는 모듈 없음")

    return {
        "rakuten": rakuten_result,
        "google": google_result,
        "combined": combined_result,
    }


# ═══════════════════════════════════════════════
# PHASE B: KJ9603 상품 매칭
# ═══════════════════════════════════════════════
def phase_b(phase_a_result):
    """PHASE B: KJ9603 카테고리 순회 + 키워드 검색 + Featured 가점"""
    logger.info("=" * 60)
    logger.info("PHASE B: KJ9603 상품 매칭 시작")
    logger.info("=" * 60)

    # 소싱 키워드 추출
    sourcing_keywords = None
    if phase_a_result and phase_a_result.get("combined"):
        sourcing_keywords = phase_a_result["combined"].get("sourcing_keywords", [])
        logger.info(f"Phase A 소싱 키워드: {len(sourcing_keywords)}개")

    # WebDriver 세션 생성
    driver = get_session()

    try:
        # ── run_phase_b: 카테고리 + 키워드 + Featured 통합 ──
        all_items = run_phase_b(
            driver,
            sourcing_keywords=sourcing_keywords,
            max_cat_pages=2,
            max_search_pages=2
        )
        logger.info(f"PHASE B 수집 완료: {len(all_items)}건")

        # ── PASS 1 필터 ──
        pass1_items = filter_pass1(all_items)
        logger.info(f"PASS 1 통과: {pass1_items and len(pass1_items)}건")

    except Exception as e:
        logger.error(f"PHASE B 오류: {e}", exc_info=True)
        all_items = []
        pass1_items = []

    return {
        "all_items": all_items,
        "pass1_items": pass1_items,
        "driver": driver,  # Phase C에서 재사용
    }


# ═══════════════════════════════════════════════
# PHASE C: 가격·경쟁·스코어링
# ═══════════════════════════════════════════════
def phase_c(phase_b_result):
    """PHASE C: 상세 크롤링 + 가격 계산 + 경쟁가 + 스코어링"""
    logger.info("=" * 60)
    logger.info("PHASE C: 가격·경쟁·스코어링 시작")
    logger.info("=" * 60)

    pass1_items = phase_b_result.get("pass1_items", [])
    driver = phase_b_result.get("driver")

    if not pass1_items:
        logger.warning("PHASE C 스킵: PASS 1 통과 상품 0건")
        return {"scored_items": [], "final_candidates": []}

    # ── C1: 상세 크롤링 (상위 PASS2_LIMIT건) ──
    logger.info(f"── PHASE C1: 상세 크롤링 (최대 {PASS2_LIMIT}건) ──")
    try:
        detailed_items = fetch_items_detail_batch(driver, pass1_items, limit=PASS2_LIMIT)
        logger.info(f"상세 크롤링 완료: {len(detailed_items)}건")
    except Exception as e:
        logger.error(f"C1 상세 크롤링 오류: {e}")
        detailed_items = pass1_items[:PASS2_LIMIT]

    # ── 0원 필터 추가 ──
    detailed_items = [i for i in detailed_items if i.get("supply_price", 0) > 0]
    logger.info(f"가격 파싱 성공: {len(detailed_items)}건") 

    # ── C2: 가격 계산 ──
    if calculate_prices_batch:
        try:
            logger.info("── PHASE C2: 가격 계산 ──")
            calculate_prices_batch(detailed_items)
            profitable = [i for i in detailed_items if i.get("is_profitable")]
            logger.info(f"수익성 있는 상품: {len(profitable)}건 / {len(detailed_items)}건")
        except Exception as e:
            logger.error(f"C2 가격 계산 오류: {e}")
    else:
        logger.warning("C2 스킵: price_calculator 모듈 없음")

    # ── C3: 경쟁가 비교 ──
    if get_competitor_prices_batch:
        try:
            logger.info("── PHASE C3: 경쟁가 비교 ──")
            # 수익성 있는 상품만 경쟁가 조회 (API 비용 절약)
            target_items = [i for i in detailed_items if i.get("is_profitable")]
            if target_items:
                get_competitor_prices_batch(target_items)
                logger.info(f"경쟁가 조회 완료: {len(target_items)}건")
            else:
                logger.warning("경쟁가 조회 스킵: 수익성 상품 0건")
        except Exception as e:
            logger.error(f"C3 경쟁가 비교 오류: {e}")
    else:
        logger.warning("C3 스킵: competitor_price 모듈 없음")

    # ── C4: 스코어링 ──
    scored_items = []
    final_candidates = []
    if calculate_scores_batch and get_final_candidates:
        try:
            logger.info("── PHASE C4: 스코어링 ──")
            
            # 트렌드 데이터 → 개별 아이템에 매핑
            sourcing_keywords = []
            if phase_a_result and phase_a_result.get("combined"):
                sourcing_keywords = phase_a_result["combined"].get("sourcing_keywords", [])
            
            if sourcing_keywords:
                trend_map = {}
                for sk in sourcing_keywords:
                    kr = sk.get("keyword_kr", "")
                    jp = sk.get("keyword_jp", "")
                    score = sk.get("combined_score", 0)
                    rank = sk.get("demand_rank", 999)
                    if kr:
                        trend_map[kr.lower()] = {"combined_score": score, "demand_rank": rank}
                    if jp:
                        trend_map[jp.lower()] = {"combined_score": score, "demand_rank": rank}
                
                matched = 0
                for item in detailed_items:
                    item_name = item.get("name", "").lower()
                    item_keywords = item.get("search_keywords", [])
                    item_keyword = item.get("search_keyword", "")
                    
                    best_score = 0
                    best_rank = 999
                    
                    for kw, tdata in trend_map.items():
                        if kw in item_name:
                            if tdata["combined_score"] > best_score:
                                best_score = tdata["combined_score"]
                                best_rank = tdata["demand_rank"]
                    
                    for kw in ([item_keyword] + list(item_keywords)):
                        kw_lower = kw.lower() if kw else ""
                        if kw_lower in trend_map:
                            tdata = trend_map[kw_lower]
                            if tdata["combined_score"] > best_score:
                                best_score = tdata["combined_score"]
                                best_rank = tdata["demand_rank"]
                    
                    if best_score > 0:
                        item["combined_score"] = best_score
                        item["demand_rank"] = best_rank
                        matched += 1
                
                logger.info(f"트렌드 매핑: {matched}/{len(detailed_items)}건 매칭")
            
            scored_items = calculate_scores_batch(detailed_items)
            final_candidates = get_final_candidates(scored_items)
            logger.info(f"스코어링 완료: {len(scored_items)}건, 최종 후보: {len(final_candidates)}건")
        except Exception as e:
            logger.error(f"C4 스코어링 오류: {e}")
    else:
        logger.warning("C4 스킵: scorer 모듈 없음")

    return {
        "detailed_items": detailed_items,
        "scored_items": scored_items,
        "final_candidates": final_candidates,
    }


# ═══════════════════════════════════════════════
# PHASE D: 번역·출품
# ═══════════════════════════════════════════════
def phase_d(phase_a_result, phase_b_result, phase_c_result):
    """PHASE D: 번역 + Sheets 업데이트 + Excel 생성 + Drive 업로드"""
    logger.info("=" * 60)
    logger.info("PHASE D: 번역·출품 시작")
    logger.info("=" * 60)

    final_candidates = phase_c_result.get("final_candidates", [])
    upload_url = None

    if not final_candidates:
        logger.warning("PHASE D: 최종 후보 0건 — 번역/출품 스킵")
    else:
        # ── D1: 일본어 번역 ──
        if translate_items_batch and generate_detail_html_batch:
            try:
                logger.info(f"── PHASE D1: 일본어 번역 ({len(final_candidates)}건) ──")
                translate_items_batch(final_candidates)
                generate_detail_html_batch(final_candidates)
                logger.info("번역 완료")
            except Exception as e:
                logger.error(f"D1 번역 오류: {e}")
        else:
            logger.warning("D1 스킵: translator 모듈 없음")

    # ── D2: Google Sheets 업데이트 (후보 유무 관계없이 실행) ──
    if update_all_sheets:
        try:
            logger.info("── PHASE D2: Google Sheets 업데이트 ──")
            update_all_sheets(
                trend_data=phase_a_result,
                scored_items=phase_c_result.get("scored_items", []),
                final_candidates=final_candidates,
                run_summary={
                    "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "phase_a": {"rakuten_scanned": phase_a_result.get("rakuten", {}).get("total_scanned", 0)},
                    "phase_b": {"collected": phase_b_result.get("total_collected", 0)},
                    "phase_c": {"candidates": len(final_candidates)},
                },
            )
            logger.info("Sheets 업데이트 완료")
        except Exception as e:
            logger.error(f"D2 Sheets 오류: {e}")
    else:
        logger.warning("D2 스킵: sheet_updater 모듈 없음")

    # ── D3: Qoo10 Excel 생성 & Drive 업로드 ──
    if final_candidates and generate_and_upload:
        try:
            logger.info(f"── PHASE D3: Excel 생성 & 업로드 ({len(final_candidates)}건) ──")
            upload_result = generate_and_upload(final_candidates)
            upload_url = upload_result.get("drive_url") if upload_result else None
            logger.info(f"업로드 완료: {upload_url}")
        except Exception as e:
            logger.error(f"D3 업로드 오류: {e}")
    else:
        if not final_candidates:
            logger.info("D3 스킵: 출품 대상 0건")
        else:
            logger.warning("D3 스킵: uploader_qoo10 모듈 없음")

    return {"upload_url": upload_url}


# ═══════════════════════════════════════════════
# 파이프라인 실행 & 리포트
# ═══════════════════════════════════════════════
def _build_report(phase_a_result, phase_b_result, phase_c_result, phase_d_result, duration):
    """최종 실행 리포트 생성"""
    report = {
        "timestamp": datetime.now().isoformat(),
        "duration_seconds": round(duration, 1),
        "status": "completed",
    }

    # Phase A 통계
    if phase_a_result and phase_a_result.get("combined"):
        combined = phase_a_result["combined"]
        report["phase_a"] = {
            "rakuten_scan": phase_a_result.get("rakuten", {}).get("summary", {}).get("total_items_scanned", 0),
            "korean_cosmetics": phase_a_result.get("rakuten", {}).get("summary", {}).get("korean_items", 0),
            "rising_keywords": combined.get("summary", {}).get("rising_keywords_count", 0),
            "sourcing_keywords": combined.get("summary", {}).get("sourcing_keywords_count", 0),
        }
    else:
        report["phase_a"] = {"status": "skipped_or_failed"}

    # Phase B 통계
    all_items = phase_b_result.get("all_items", [])
    pass1_items = phase_b_result.get("pass1_items", [])
    report["phase_b"] = {
        "total_collected": len(all_items),
        "pass1_passed": len(pass1_items),
        "badge_items": len([i for i in all_items if i.get("badge_bonus", 0) > 0]),
    }

    # Phase C 통계
    detailed = phase_c_result.get("detailed_items", [])
    scored = phase_c_result.get("scored_items", [])
    final = phase_c_result.get("final_candidates", [])
    report["phase_c"] = {
        "detailed_crawl": len(detailed),
        "profitable": len([i for i in detailed if i.get("is_profitable")]),
        "scored": len(scored),
        "final_candidates": len(final),
    }

    # Phase D 통계
    report["phase_d"] = {
        "upload_count": len(final),
        "drive_url": phase_d_result.get("upload_url"),
    }

    # 최종 상태 판단
    if len(final) > 0:
        report["status"] = "success"
    elif len(pass1_items) > 0:
        report["status"] = "no_final_candidates"
    elif len(all_items) > 0:
        report["status"] = "no_pass1_items"
    else:
        report["status"] = "no_items"

    return report


def _print_report(report):
    """리포트 출력"""
    logger.info("=" * 60)
    logger.info("파이프라인 실행 완료 리포트")
    logger.info("=" * 60)
    logger.info(f"상태: {report['status']}")
    logger.info(f"실행 시간: {report['duration_seconds']}초 ({report['duration_seconds']/60:.1f}분)")

    if "phase_a" in report and isinstance(report["phase_a"], dict):
        a = report["phase_a"]
        if a.get("rakuten_scan"):
            logger.info(f"[Phase A] Rakuten: {a.get('rakuten_scan', 0)}건 스캔, "
                       f"한국코스메: {a.get('korean_cosmetics', 0)}건, "
                       f"소싱 키워드: {a.get('sourcing_keywords', 0)}개")

    b = report.get("phase_b", {})
    logger.info(f"[Phase B] 수집: {b.get('total_collected', 0)}건, "
               f"PASS1: {b.get('pass1_passed', 0)}건, "
               f"배지상품: {b.get('badge_items', 0)}건")

    c = report.get("phase_c", {})
    logger.info(f"[Phase C] 상세: {c.get('detailed_crawl', 0)}건, "
               f"수익성: {c.get('profitable', 0)}건, "
               f"최종후보: {c.get('final_candidates', 0)}건")

    d = report.get("phase_d", {})
    logger.info(f"[Phase D] 업로드: {d.get('upload_count', 0)}건, "
               f"URL: {d.get('drive_url', 'N/A')}")

    # Top 10 후보 출력
    logger.info("-" * 60)


def main():
    """메인 파이프라인 실행"""
    start_time = time.time()

    logger.info("=" * 60)
    logger.info("Qoo10 Japan Beauty Sourcing Pipeline v0.8")
    logger.info(f"실행 시작: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    phase_a_result = None
    phase_b_result = {"all_items": [], "pass1_items": [], "driver": None}
    phase_c_result = {"detailed_items": [], "scored_items": [], "final_candidates": []}
    phase_d_result = {"upload_url": None}

    try:
        # ── PHASE A ──
        phase_a_result = phase_a()

        # ── PHASE B ──
        phase_b_result = phase_b(phase_a_result)

        # ── PHASE C ──
        phase_c_result = phase_c(phase_b_result)

        # ── PHASE D ──
        phase_d_result = phase_d(phase_a_result, phase_b_result, phase_c_result)

    except Exception as e:
        logger.error(f"파이프라인 치명적 오류: {e}", exc_info=True)

    finally:
        # WebDriver 종료
        try:
            close_driver()
            logger.info("WebDriver 종료 완료")
        except Exception:
            pass

    # ── 리포트 ──
    duration = time.time() - start_time
    report = _build_report(phase_a_result, phase_b_result, phase_c_result, phase_d_result, duration)
    _print_report(report)

    # 리포트 JSON 저장 (디버깅용)
    try:
        with open("run_report.json", "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info("리포트 저장: run_report.json")
    except Exception:
        pass

    return report


if __name__ == "__main__":
    report = main()
    # 최종 후보 0건이어도 정상 종료 (exit 0)
    # Phase B에서 상품 수집 자체가 실패한 경우만 exit 1
    if report.get("status") == "no_items" and report.get("phase_b", {}).get("total_collected", 0) == 0:
        logger.warning("상품 수집 실패 — 셀렉터 또는 로그인 확인 필요")
    sys.exit(0)
