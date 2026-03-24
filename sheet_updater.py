"""
sheet_updater.py – Google Sheets 업데이트 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.8.2

★ v0.8.1 → v0.8.2 변경사항:
  1. [BUG FIX] update_upload_sheet / update_sourcing_sheet 분리
     → 큐텐업로드 시트에 소싱 분석 데이터가 중복 삽입되던 문제 해결
     → 81건이 162건으로 기록되던 버그 수정
  2. update_all_sheets 호출 구조 명확화

기존 유지 (v0.8.1):
  - 50컬럼 대응 (H: start_date, L: taxrate)
  - 추가이미지 구분자 || → $$
  - 검색키워드 구분자 , → $$ + 각 30글자 제한
  - 상품명 홍보성 금지 문구 자동 제거
  - available_shipping_date: 一般発送 → 3
  - end_date: 2030-12-31 고정
"""

import os
import json
import math
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ── 설정 ───────────────────────────
QOO10_SHEET_ID = os.environ.get("QOO10_SHEET_ID", "")
QOO10_KSE_SHIPPING_CODE = os.environ.get("QOO10_KSE_SHIPPING_CODE", "813137")

# ── uploader_qoo10 매핑 함수 import (실패 시 로컬 최소 구현) ──
try:
    from uploader_qoo10 import (
        _match_qoo10_category,
        _match_brand_code,
        _get_item_weight,
        _truncate_item_name,
        _extract_search_keywords,
        OFFICIAL_HEADERS,
        FORBIDDEN_PROMO_WORDS,
        QOO10_KSE_SHIPPING_CODE as UPLOADER_SHIPPING_CODE,
    )
    UPLOADER_IMPORTED = True
    logger.info("[import] uploader_qoo10 매핑 함수 로드 완료")
except ImportError:
    UPLOADER_IMPORTED = False
    logger.warning("[import] uploader_qoo10 import 실패 – 로컬 최소 구현 사용")

    OFFICIAL_HEADERS = [
        "item_number", "seller_unique_item_id", "category_number", "brand_number",
        "item_name", "item_promotion_name", "item_status", "start_date", "end_date",
        "price_yen", "retail_price_yen", "taxrate", "quantity", "option_info",
        "additional_option_info", "additional_option_text", "image_main_url",
        "image_other_url", "video_url", "image_option_info",
        "image_additional_option_info", "header_html", "footer_html",
        "item_description", "Shipping_number", "option_number",
        "available_shipping_date", "desired_shipping_date", "search_keyword",
        "item_condition_type", "origin_type", "origin_region_id",
        "origin_country_id", "origin_others", "medication_type", "item_weight",
        "item_material", "model_name", "external_product_type", "external_product_id",
        "manufacture_date", "expiration_date_type", "expiration_date_MFD",
        "expiration_date_PAO", "expiration_date_EXP", "under18s_display",
        "A/S_info", "buy_limit_type", "buy_limit_date", "buy_limit_qty",
    ]
    FORBIDDEN_PROMO_WORDS = [
        "割引", "特価", "セール", "SALE", "sale", "Sale",
        "激安", "最安", "最安値", "格安", "限定", "期間限定",
        "送料無料", "無料配送", "ポイント", "クーポン",
        "1+1", "2+1", "おまけ", "人気No.1", "ランキング1位", "売れ筋",
    ]

    def _match_qoo10_category(item):
        return item.get("qoo10_category", "100000043")

    def _match_brand_code(item):
        return item.get("qoo10_brand", "")

    def _get_item_weight(cat):
        return 300

    def _truncate_item_name(name, max_len=50):
        if not name:
            return ""
        for w in FORBIDDEN_PROMO_WORDS:
            name = name.replace(w, "")
        name = re.sub(r'\s+', ' ', name).strip()
        if len(name) > max_len:
            name = name[:max_len - 1] + "…"
        return name

    def _extract_search_keywords(item, cat=""):
        brand = item.get("brand", "")
        name = item.get("name_jp", item.get("name", ""))
        kw = f"{brand} {name}".strip()
        return kw[:30] if len(kw) > 30 else kw


# ══════════════════════════════════════════════════════════════
# Google Sheets 클라이언트
# ══════════════════════════════════════════════════════════════

def _get_sheets_client():
    """gspread 클라이언트 반환"""
    try:
        import gspread
        from google.oauth2 import service_account

        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not sa_json:
            raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON 환경변수 없음")

        sa_info = json.loads(sa_json)
        credentials = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        return gspread.authorize(credentials)
    except Exception as e:
        logger.error(f"[Sheets] 클라이언트 생성 실패: {e}")
        raise


def _get_spreadsheet():
    """스프레드시트 객체 반환"""
    client = _get_sheets_client()
    if not QOO10_SHEET_ID:
        raise ValueError("QOO10_SHEET_ID 환경변수 없음")
    return client.open_by_key(QOO10_SHEET_ID)


def _get_or_create_worksheet(spreadsheet, title, rows=1000, cols=50):
    """워크시트 가져오기 (없으면 생성)"""
    try:
        return spreadsheet.worksheet(title)
    except Exception:
        logger.info(f"[Sheets] '{title}' 시트 생성")
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def _ensure_rows(worksheet, needed_rows):
    """행 수가 부족하면 확장"""
    current = worksheet.row_count
    if needed_rows > current:
        worksheet.add_rows(needed_rows - current)
        logger.info(f"[Sheets] 행 확장: {current} → {needed_rows}")


def _ensure_cols(worksheet, needed_cols):
    """열 수가 부족하면 확장"""
    current = worksheet.col_count
    if needed_cols > current:
        worksheet.add_cols(needed_cols - current)
        logger.info(f"[Sheets] 열 확장: {current} → {needed_cols}")


# ══════════════════════════════════════════════════════════════
# 50컬럼 업로드 행 빌더
# ══════════════════════════════════════════════════════════════

def _build_upload_row(item: dict) -> list:
    """
    item dict → Qoo10 공식 50컬럼 리스트 (A~AX)
    ★ v0.8.2: 이 함수는 큐텐업로드 시트 전용
    """
    qoo10_cat = _match_qoo10_category(item)
    qoo10_brand = _match_brand_code(item)
    weight = _get_item_weight(qoo10_cat)

    # 이미지
    thumbnail = item.get("thumbnail", "") or item.get("image_url", "")
    detail_images = item.get("detail_images", [])
    other_images = "$$".join(detail_images[:20]) if detail_images else ""

    # 옵션
    detail = item.get("detail_info", {})
    opt_name = detail.get("option1_name", "")
    opt_values = detail.get("option1_values", [])
    option_str = ""
    if opt_name and opt_values:
        sp = item.get("sell_price_jpy", 0)
        option_parts = [f"{v}^{sp}^99^0^0" for v in opt_values]
        option_str = f"{opt_name}:#{'$$'.join(option_parts)}"

    # 가격
    sell_price = item.get("sell_price_jpy", 0)
    retail_price = item.get("consumer_price_jpy", 0)
    if retail_price <= sell_price:
        retail_price = math.ceil(sell_price * 1.3)

    # 상품명
    name_jp = item.get("name_jp", item.get("name", ""))
    item_name = _truncate_item_name(name_jp)

    # 검색어
    search_kw = _extract_search_keywords(item, qoo10_cat)

    # seller_unique_item_id
    seller_uid = f"KJ{item.get('item_id', '') or item.get('product_id', '')}"

    # header / footer
    header_html = item.get("header_html", "")
    footer_html = item.get("footer_html", "")

    return [
        '',                                                  # A  item_number
        seller_uid,                                          # B  seller_unique_item_id
        str(qoo10_cat),                                      # C  category_number
        str(qoo10_brand),                                    # D  brand_number
        item_name,                                           # E  item_name
        '韓国コスメ 正規品',                                    # F  item_promotion_name
        'Y',                                                 # G  item_status
        '',                                                  # H  start_date
        '2030-12-31',                                        # I  end_date
        str(sell_price),                                     # J  price_yen
        str(retail_price),                                   # K  retail_price_yen
        '',                                                  # L  taxrate
        '100',                                               # M  quantity
        option_str,                                          # N  option_info
        '',                                                  # O  additional_option_info
        '',                                                  # P  additional_option_text
        item.get('thumbnail_processed', '') or thumbnail,    # Q  image_main_url
        other_images,                                        # R  image_other_url
        '',                                                  # S  video_url
        '',                                                  # T  image_option_info
        '',                                                  # U  image_additional_option_info
        header_html,                                         # V  header_html
        footer_html,                                         # W  footer_html
        item.get('detail_html', ''),                         # X  item_description
        QOO10_KSE_SHIPPING_CODE,                             # Y  Shipping_number
        '',                                                  # Z  option_number
        '3',                                                 # AA available_shipping_date
        '7',                                                 # AB desired_shipping_date
        search_kw,                                           # AC search_keyword
        '1',                                                 # AD item_condition_type
        '2',                                                 # AE origin_type
        '',                                                  # AF origin_region_id
        'KR',                                                # AG origin_country_id
        '',                                                  # AH origin_others
        '',                                                  # AI medication_type
        str(weight),                                         # AJ item_weight
        '',                                                  # AK item_material
        '',                                                  # AL model_name
        '',                                                  # AM external_product_type
        '',                                                  # AN external_product_id
        '',                                                  # AO manufacture_date
        '',                                                  # AP expiration_date_type
        '',                                                  # AQ expiration_date_MFD
        '',                                                  # AR expiration_date_PAO
        '',                                                  # AS expiration_date_EXP
        'N',                                                 # AT under18s_display
        '',                                                  # AU A/S_info
        '',                                                  # AV buy_limit_type
        '',                                                  # AW buy_limit_date
        '',                                                  # AX buy_limit_qty
    ]


# ══════════════════════════════════════════════════════════════
# 소싱후보 행 빌더 (★ v0.8.2 신규 – 기존 루프에서 분리)
# ══════════════════════════════════════════════════════════════

def _build_sourcing_row(item: dict, timestamp: str) -> list:
    """
    item dict → 소싱후보 시트 27컬럼 리스트
    ★ v0.8.2: update_upload_sheet 루프에서 분리
    """
    pi = item.get("price_info", {})
    si = item.get("score_info", {})
    comp = item.get("competitor_prices", {})

    rakuten_low = comp.get("rakuten_lowest", "")
    kakaku_low = comp.get("kakaku_lowest", "")
    qoo10_low = comp.get("qoo10_lowest", "")

    return [
        timestamp,
        item.get("item_id", ""),
        item.get("name", ""),
        item.get("name_jp", ""),
        item.get("brand", ""),
        item.get("supply_price", ""),
        item.get("kj_shipping", ""),
        pi.get("total_cost_jpy", ""),
        pi.get("kse_fee_jpy", ""),
        item.get("sell_price_jpy", ""),
        pi.get("margin_jpy", ""),
        f"{item.get('margin_rate', 0) * 100:.1f}%",
        rakuten_low,
        kakaku_low,
        qoo10_low,
        item.get("competitor_lowest_jpy", ""),
        si.get("price_score", ""),
        si.get("trend_score", ""),
        si.get("demand_score", ""),
        si.get("platform_score", ""),
        item.get("final_score", ""),
        item.get("grade", ""),
        "✅" if item.get("pass_final") else "❌",
        item.get("trend_keyword_jp", ""),
        item.get("demand_rank", ""),
        len(item.get("detail_images", [])),
        item.get("url", ""),
    ]


# ══════════════════════════════════════════════════════════════
# 시트 업데이트 함수들
# ══════════════════════════════════════════════════════════════

SOURCING_HEADERS = [
    "timestamp", "item_id", "name", "name_jp", "brand",
    "supply_price", "kj_shipping", "total_cost_jpy", "kse_fee_jpy",
    "sell_price_jpy", "margin_jpy", "margin_rate",
    "rakuten_lowest", "kakaku_lowest", "qoo10_lowest",
    "competitor_lowest_jpy",
    "price_score", "trend_score", "demand_score", "platform_score",
    "final_score", "grade", "pass_final",
    "trend_keyword_jp", "demand_rank", "image_count", "url",
]

TREND_HEADERS = [
    "timestamp", "source", "demand_rank", "keyword_jp", "keyword_kr_placeholder",
    "keyword_type", "avg_price_jpy", "total_reviews", "placeholder",
    "keyword_jp_2", "keyword_kr", "combined_score", "extra1", "extra2",
]


def update_trend_sheet(trend_data: dict):
    """트렌드 분석 시트 업데이트"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for item in trend_data.get("sourcing_keywords", []):
        rows.append([
            timestamp,
            "combined",
            item.get("demand_rank", ""),
            item.get("keyword_jp", ""),
            "",
            item.get("keyword_type", ""),
            item.get("avg_price_jpy", ""),
            item.get("total_reviews", ""),
            "",
            item.get("keyword_jp", ""),
            item.get("keyword_kr", ""),
            item.get("combined_score", ""),
            "",
            "",
        ])

    if not rows:
        logger.info("[트렌드] 데이터 없음 – 스킵")
        return 0

    try:
        ss = _get_spreadsheet()
        ws = _get_or_create_worksheet(ss, "트렌드분석", rows=2000, cols=20)

        # 헤더 확인
        existing = ws.row_values(1)
        if not existing or len(existing) < len(TREND_HEADERS):
            ws.update('A1', [TREND_HEADERS])

        # 데이터 추가
        next_row = len(ws.get_all_values()) + 1
        _ensure_rows(ws, next_row + len(rows))
        ws.update(f'A{next_row}', rows)

        logger.info(f"[트렌드] {len(rows)}행 추가 완료")
        return len(rows)

    except Exception as e:
        logger.error(f"[트렌드] 시트 업데이트 실패: {e}")
        return 0


def update_sourcing_sheet(items: list):
    """
    소싱후보 시트 업데이트 (27컬럼 분석 데이터)
    ★ v0.8.2: 큐텐업로드와 완전 분리
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for item in items:
        rows.append(_build_sourcing_row(item, timestamp))

    if not rows:
        logger.info("[소싱후보] 데이터 없음 – 스킵")
        return 0

    try:
        ss = _get_spreadsheet()
        ws = _get_or_create_worksheet(ss, "소싱후보", rows=2000, cols=30)

        # 헤더 확인
        existing = ws.row_values(1)
        if not existing or len(existing) < len(SOURCING_HEADERS):
            ws.update('A1', [SOURCING_HEADERS])

        # 데이터 추가
        next_row = len(ws.get_all_values()) + 1
        _ensure_rows(ws, next_row + len(rows))
        ws.update(f'A{next_row}', rows)

        logger.info(f"[소싱후보] {len(rows)}행 추가 완료")
        return len(rows)

    except Exception as e:
        logger.error(f"[소싱후보] 시트 업데이트 실패: {e}")
        return 0


def update_upload_sheet(items: list):
    """
    큐텐업로드 시트 업데이트 (공식 50컬럼)
    ★ v0.8.2: _build_upload_row만 사용, 소싱 분석 행 제거
    """
    # ── 50컬럼 업로드 행만 생성 ──
    upload_rows = []
    for item in items:
        upload_rows.append(_build_upload_row(item))

    if not upload_rows:
        logger.info("[큐텐업로드] 데이터 없음 – 스킵")
        return {"count": 0, "brand_matched": 0, "cat_top5": {}, "weight_dist": {}}

    # ── 통계 ──
    brand_matched = 0
    cat_stats = {}
    weight_stats = {}

    for item in items:
        qoo10_cat = _match_qoo10_category(item)
        cat_stats[qoo10_cat] = cat_stats.get(qoo10_cat, 0) + 1

        qoo10_brand = _match_brand_code(item)
        if qoo10_brand:
            brand_matched += 1

        weight = _get_item_weight(qoo10_cat)
        weight_key = str(weight)
        weight_stats[weight_key] = weight_stats.get(weight_key, 0) + 1

    try:
        ss = _get_spreadsheet()
        ws = _get_or_create_worksheet(ss, "큐텐업로드", rows=2000, cols=55)

        # 열 수 확보
        _ensure_cols(ws, len(OFFICIAL_HEADERS))

        # 헤더 확인
        existing = ws.row_values(1)
        if not existing or len(existing) < len(OFFICIAL_HEADERS):
            ws.update('A1', [OFFICIAL_HEADERS])
            logger.info(f"[큐텐업로드] 헤더 설정 완료: {len(OFFICIAL_HEADERS)}컬럼")

        # 데이터 추가
        next_row = len(ws.get_all_values()) + 1
        _ensure_rows(ws, next_row + len(upload_rows))
        ws.update(f'A{next_row}', upload_rows)

        top5_cat = dict(sorted(cat_stats.items(), key=lambda x: -x[1])[:5])

        logger.info(f"[큐텐업로드] {len(upload_rows)}행 추가 완료 (50컬럼)")
        logger.info(f"[큐텐업로드] 브랜드 매칭: {brand_matched}/{len(items)}건")
        logger.info(f"[큐텐업로드] 카테고리 TOP5: {top5_cat}")
        logger.info(f"[큐텐업로드] 무게 분포: {weight_stats}")

        return {
            "count": len(upload_rows),
            "brand_matched": brand_matched,
            "cat_top5": top5_cat,
            "weight_dist": weight_stats,
        }

    except Exception as e:
        logger.error(f"[큐텐업로드] 시트 업데이트 실패: {e}")
        return {"count": 0, "brand_matched": 0, "cat_top5": {}, "weight_dist": {}}


def log_run_info(summary: dict):
    """파이프라인 실행 요약을 운영정보 시트에 기록"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for key, value in summary.items():
        rows.append([timestamp, str(key), str(value)])

    if not rows:
        return 0

    try:
        ss = _get_spreadsheet()
        ws = _get_or_create_worksheet(ss, "운영정보", rows=2000, cols=10)

        # 헤더 확인
        existing = ws.row_values(1)
        if not existing:
            ws.update('A1', [["timestamp", "key", "value"]])

        next_row = len(ws.get_all_values()) + 1
        _ensure_rows(ws, next_row + len(rows))
        ws.update(f'A{next_row}', rows)

        logger.info(f"[운영정보] {len(rows)}행 추가 완료")
        return len(rows)

    except Exception as e:
        logger.error(f"[운영정보] 시트 업데이트 실패: {e}")
        return 0


# ══════════════════════════════════════════════════════════════
# 통합 업데이트
# ══════════════════════════════════════════════════════════════

def update_all_sheets(trend_data: dict, scored_items: list,
                      final_candidates: list, run_summary: dict) -> dict:
    """
    모든 시트 업데이트 통합 호출
    ★ v0.8.2: 각 시트별 함수가 명확히 분리되어 중복 없음

    - 트렌드분석: trend_data (sourcing_keywords)
    - 소싱후보: scored_items (전체 분석 대상, 27컬럼)
    - 큐텐업로드: final_candidates (최종 통과, 50컬럼)
    - 운영정보: run_summary (파이프라인 로그)
    """
    results = {}

    # 1. 트렌드 분석
    try:
        trend_count = update_trend_sheet(trend_data)
        results["trend_rows"] = trend_count
        logger.info(f"[전체] 트렌드분석: {trend_count}행")
    except Exception as e:
        logger.error(f"[전체] 트렌드 업데이트 실패: {e}")
        results["trend_rows"] = 0

    # 2. 소싱후보 (scored_items → 27컬럼 분석 데이터)
    try:
        sourcing_count = update_sourcing_sheet(scored_items)
        results["sourcing_rows"] = sourcing_count
        logger.info(f"[전체] 소싱후보: {sourcing_count}행")
    except Exception as e:
        logger.error(f"[전체] 소싱후보 업데이트 실패: {e}")
        results["sourcing_rows"] = 0

    # 3. 큐텐업로드 (final_candidates → 50컬럼 등록 데이터)
    try:
        upload_result = update_upload_sheet(final_candidates)
        results["upload_rows"] = upload_result.get("count", 0)
        results["brand_matched"] = upload_result.get("brand_matched", 0)
        results["cat_top5"] = upload_result.get("cat_top5", {})
        results["weight_dist"] = upload_result.get("weight_dist", {})
        logger.info(f"[전체] 큐텐업로드: {results['upload_rows']}행")
    except Exception as e:
        logger.error(f"[전체] 큐텐업로드 업데이트 실패: {e}")
        results["upload_rows"] = 0

    # 4. 운영정보
    try:
        log_count = log_run_info(run_summary)
        results["log_rows"] = log_count
        logger.info(f"[전체] 운영정보: {log_count}행")
    except Exception as e:
        logger.error(f"[전체] 운영정보 업데이트 실패: {e}")
        results["log_rows"] = 0

    logger.info(f"[전체] 시트 업데이트 완료: {results}")
    return results


# ══════════════════════════════════════════════════════════════
# 직접 실행 (검증용)
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    print("=== sheet_updater.py v0.8.2 검증 ===")
    print(f"QOO10_SHEET_ID: {'설정됨' if QOO10_SHEET_ID else '미설정'}")
    print(f"uploader_qoo10 import: {'성공' if UPLOADER_IMPORTED else '실패 (로컬 구현 사용)'}")
    print(f"OFFICIAL_HEADERS: {len(OFFICIAL_HEADERS)}컬럼")
    print()

    if QOO10_SHEET_ID:
        try:
            ss = _get_spreadsheet()
            print(f"스프레드시트 연결 성공: {ss.title}")
            for ws in ss.worksheets():
                row_count = len(ws.get_all_values())
                print(f"  - {ws.title}: {row_count}행, {ws.col_count}열")
        except Exception as e:
            print(f"스프레드시트 연결 실패: {e}")
    else:
        print("QOO10_SHEET_ID 환경변수가 없어 시트 연결을 건너뜁니다.")

    print()
    print("★ v0.8.2 핵심 수정: 큐텐업로드 시트에 소싱 분석 행이 중복 삽입되던 버그 수정")
    print("  - update_upload_sheet: _build_upload_row만 사용 (50컬럼)")
    print("  - update_sourcing_sheet: _build_sourcing_row만 사용 (27컬럼)")
    print("  - 81건 입력 → 81행 출력 (기존 162행 → 81행)")