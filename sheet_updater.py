"""
sheet_updater.py – Google Sheets 업데이트 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.8.1

★ v0.8 → v0.8.1 변경사항:
  1. 50컬럼 대응 (H: start_date, L: taxrate 추가)
  2. 추가이미지 구분자 || → $$ 
  3. 검색키워드 구분자 , → $$ + 각 30글자 제한
  4. 상품명 홍보성 금지 문구 자동 제거
  5. available_shipping_date: 一般発送 → 3
  6. end_date: 2030-12-31 고정
"""

import os
import json
import math
import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────────────
QOO10_SHEET_ID = os.environ.get("QOO10_SHEET_ID", "")
QOO10_KSE_SHIPPING_CODE = os.environ.get("QOO10_KSE_SHIPPING_CODE", "813137")


# ══════════════════════════════════════════════════════════════
# uploader_qoo10.py에서 매핑 함수 import (★ v0.8)
# ══════════════════════════════════════════════════════════════

try:
    from uploader_qoo10 import (
        _match_qoo10_category,
        _match_brand_code,
        _get_item_weight,
        _truncate_item_name,
        _remove_prohibited_words,
        _extract_search_keywords,
        OFFICIAL_HEADERS,
        CATEGORY_JP_NAME,
    )
    logger.info("[시트] uploader_qoo10 매핑 함수 import 성공")
    _USE_UPLOADER_IMPORT = True
except ImportError:
    logger.warning("[시트] uploader_qoo10 import 실패 → 로컬 폴백 사용")
    _USE_UPLOADER_IMPORT = False

    # ── 폴백: 최소한의 매핑 ─────────
    OFFICIAL_HEADERS = [
        'item_number', 'seller_unique_item_id', 'category_number',
        'brand_number', 'item_name', 'item_promotion_name',
        'item_status_Y/N/D', 'start_date', 'end_date',
        'price_yen', 'retail_price_yen', 'taxrate',
        'quantity', 'option_info', 'additional_option_info',
        'additional_option_text', 'image_main_url', 'image_other_url',
        'video_url', 'image_option_info', 'image_additional_option_info',
        'header_html', 'footer_html', 'item_description',
        'Shipping_number', 'option_number', 'available_shipping_date',
        'desired_shipping_date', 'search_keyword', 'item_condition_type',
        'origin_type', 'origin_region_id', 'origin_country_id',
        'origin_others', 'medication_type', 'item_weight',
        'item_material', 'model_name', 'external_product_type',
        'external_product_id', 'manufacture_date', 'expiration_date_type',
        'expiration_date_MFD', 'expiration_date_PAO', 'expiration_date_EXP',
        'under18s_display_Y/N', 'A/S_info', 'buy_limit_type',
        'buy_limit_date', 'buy_limit_qty',
    ]

    PROHIBITED_WORDS = [
        '特価', '割引', '破格', 'セール', 'SALE', 'sale',
        '激安', '最安', '限定', '半額', 'OFF', '%OFF',
        '送料無料', '無料配送', 'ポイント', '倍',
        '특가', '할인', '파격', '세일', '한정', '무료배송',
    ]

    def _match_qoo10_category(item):
        return "320001621"

    def _match_brand_code(item):
        return ""

    def _get_item_weight(category_code):
        return 0.50

    def _remove_prohibited_words(name):
        for word in PROHIBITED_WORDS:
            name = name.replace(word, '')
        name = re.sub(r'\[\s*\]', '', name)
        name = re.sub(r'\(\s*\)', '', name)
        name = re.sub(r'\s{2,}', ' ', name)
        return name.strip()

    def _truncate_item_name(name, max_len=50):
        if not name:
            return ""
        cleaned = re.sub(r'\[.*?(?:특가|한정|세일|할인|이벤트|봄맞이).*?\]\s*', '', name)
        cleaned = _remove_prohibited_words(cleaned)
        return cleaned[:max_len]

    def _extract_search_keywords(item, category_code):
        brand = (item.get("brand", "") or "").strip()
        parts = [brand, "韓国コスメ", "韓国", "Korean Beauty"]
        return "$$".join([p[:30] for p in parts if p][:10])

    CATEGORY_JP_NAME = {}


# ══════════════════════════════════════════════════════════════
# 공통 유틸리티
# ══════════════════════════════════════════════════════════════

def _get_sheets_client():
    """Google Sheets API 클라이언트 생성"""
    try:
        import gspread
        from google.oauth2 import service_account

        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not sa_json:
            logger.error("GOOGLE_SERVICE_ACCOUNT_JSON 미설정")
            return None

        sa_info = json.loads(sa_json)
        credentials = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=[
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive",
            ]
        )
        client = gspread.authorize(credentials)
        return client
    except ImportError:
        logger.error("gspread 미설치 – pip install gspread")
        return None
    except Exception as e:
        logger.error(f"Sheets 클라이언트 생성 실패: {e}")
        return None


def _get_spreadsheet(client):
    """스프레드시트 객체 반환"""
    if not QOO10_SHEET_ID:
        logger.error("QOO10_SHEET_ID 미설정")
        return None
    try:
        return client.open_by_key(QOO10_SHEET_ID)
    except Exception as e:
        logger.error(f"스프레드시트 열기 실패: {e}")
        return None


def _get_or_create_worksheet(spreadsheet, title, rows=1000, cols=30):
    """워크시트 반환 (없으면 생성)"""
    try:
        return spreadsheet.worksheet(title)
    except Exception:
        logger.info(f"워크시트 '{title}' 생성 (rows={rows}, cols={cols})")
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def _ensure_sheet_rows(ws, needed_rows):
    """시트 행 수가 부족하면 자동 확장"""
    try:
        current_rows = ws.row_count
        if current_rows < needed_rows:
            expand_to = needed_rows + 500
            ws.resize(rows=expand_to)
            logger.info(f"[시트] '{ws.title}' 행 확장: {current_rows} → {expand_to}")
    except Exception as e:
        logger.warning(f"[시트] '{ws.title}' 행 확장 실패: {e}")


def _ensure_sheet_cols(ws, needed_cols):
    """시트 컬럼 수가 부족하면 자동 확장"""
    try:
        current_cols = ws.col_count
        if current_cols < needed_cols:
            expand_to = needed_cols + 5
            ws.resize(cols=expand_to)
            logger.info(f"[시트] '{ws.title}' 컬럼 확장: {current_cols} → {expand_to}")
    except Exception as e:
        logger.warning(f"[시트] '{ws.title}' 컬럼 확장 실패: {e}")


# ══════════════════════════════════════════════════════════════
# ★ v0.8.1: 큐텐업로드 전용 50컬럼 행 생성
# ══════════════════════════════════════════════════════════════

def _build_upload_row(item):
    """단일 상품 dict → 공식 양식 50컬럼 리스트"""
    cat_code = _match_qoo10_category(item)
    brand_code = _match_brand_code(item)
    price = int(item.get('sell_price_jpy', 0) or item.get('price_jpy', 0) or 0)
    retail_price = math.ceil(price * 1.3) if price > 0 else 0
    weight = _get_item_weight(cat_code)
    name_jp = item.get('name_jp', '') or item.get('name', '') or ''
    item_name = _truncate_item_name(name_jp)

    # 이미지
    images = item.get('detail_images', []) or item.get('images', []) or []
    thumbnail = item.get('thumbnail', '') or item.get('image_url', '')
    main_image = thumbnail or (images[0] if images else '')
    # ★ v0.8.1: 구분자 $$
    other_images = '$$'.join(images[:20]) if images else ''

    # 상세 HTML
    detail_html = item.get('detail_html', '') or ''

    # 판매자 상품코드
    seller_id = item.get('item_id', '') or item.get('product_id', '') or ''

    # 검색어 (★ v0.8.1: $$ 구분 + 30글자 제한)
    search_kw = _extract_search_keywords(item, cat_code)

    # 옵션
    detail = item.get('detail_info', {})
    opt_name = detail.get('option1_name', '')
    opt_values = detail.get('option1_values', [])
    option_str = ''
    if opt_name and opt_values:
        option_parts = [f"{v}^{price}^99^0^0" for v in opt_values]
        option_str = f"{opt_name}:#{'$$'.join(option_parts)}"

    row = [
        '',                              # A  item_number (신규→공란)
        f'KJ{seller_id}',               # B  seller_unique_item_id
        str(cat_code),                   # C  category_number
        str(brand_code),                 # D  brand_number
        item_name,                       # E  item_name
        '韓国コスメ 正規品',               # F  item_promotion_name
        'Y',                             # G  item_status
        '',                              # H  start_date (공란=즉시)    ★ v0.8.1
        '2030-12-31',                    # I  end_date                 ★ v0.8.1
        str(price),                      # J  price_yen
        str(retail_price),               # K  retail_price_yen
        '',                              # L  taxrate (공란=기본)       ★ v0.8.1
        '100',                           # M  quantity
        option_str,                      # N  option_info
        '',                              # O  additional_option_info
        '',                              # P  additional_option_text
        main_image,                      # Q  image_main_url
        other_images,                    # R  image_other_url
        '',                              # S  video_url
        '',                              # T  image_option_info
        '',                              # U  image_additional_option_info
        '',                              # V  header_html
        '',                              # W  footer_html
        detail_html,                     # X  item_description
        QOO10_KSE_SHIPPING_CODE,         # Y  Shipping_number
        '',                              # Z  option_number
        '3',                             # AA available_shipping_date  ★ v0.8.1
        '7',                             # AB desired_shipping_date
        search_kw,                       # AC search_keyword
        '1',                             # AD item_condition_type (신품)
        '2',                             # AE origin_type (해외)
        '',                              # AF origin_region_id
        'KR',                            # AG origin_country_id
        '',                              # AH origin_others
        '',                              # AI medication_type
        str(weight),                     # AJ item_weight
        '',                              # AK item_material
        '',                              # AL model_name
        '',                              # AM external_product_type
        '',                              # AN external_product_id
        '',                              # AO manufacture_date
        '',                              # AP expiration_date_type
        '',                              # AQ expiration_date_MFD
        '',                              # AR expiration_date_PAO
        '',                              # AS expiration_date_EXP
        'N',                             # AT under18s_display
        '',                              # AU A/S_info
        '',                              # AV buy_limit_type
        '',                              # AW buy_limit_date
        '',                              # AX buy_limit_qty
    ]
    return row


# ══════════════════════════════════════════════════════════════
# 1. 트렌드분석 탭 업데이트
# ══════════════════════════════════════════════════════════════

TREND_HEADERS = [
    "수집일시", "소스", "순위", "상품명(JP)", "브랜드", "카테고리",
    "가격(JPY)", "리뷰수", "평점", "키워드(JP)", "키워드(KR)",
    "트렌드점수", "KJ9603매칭", "매칭ID"
]

def update_trend_sheet(trend_data: dict):
    """PHASE A 트렌드 분석 결과를 시트에 기록"""
    client = _get_sheets_client()
    if not client:
        return

    spreadsheet = _get_spreadsheet(client)
    if not spreadsheet:
        return

    ws = _get_or_create_worksheet(spreadsheet, "트렌드분석")
    timestamp = trend_data.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    try:
        existing = ws.row_values(1)
        if not existing or existing[0] != TREND_HEADERS[0]:
            ws.update("A1", [TREND_HEADERS])
            logger.info("[시트] 트렌드분석 헤더 설정")
    except Exception:
        ws.update("A1", [TREND_HEADERS])

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

    if rows:
        next_row = len(ws.get_all_values()) + 1
        _ensure_sheet_rows(ws, next_row + len(rows))
        ws.update(f"A{next_row}", rows)
        logger.info(f"[시트] 트렌드분석 {len(rows)}행 추가 (row {next_row}~)")


# ══════════════════════════════════════════════════════════════
# 2. 소싱후보 탭 업데이트
# ══════════════════════════════════════════════════════════════

SOURCING_HEADERS = [
    "수집일시", "상품ID", "상품명(KR)", "상품명(JP)", "브랜드",
    "공급가(KRW)", "국내배송비", "총원가(JPY)", "KSE배송비(JPY)",
    "판매가(JPY)", "마진(JPY)", "마진율",
    "라쿠텐최저가", "카카쿠최저가", "큐텐최저가", "종합최저가",
    "가격점수", "트렌드점수", "수요점수", "플랫폼점수",
    "최종점수", "등급", "통과여부",
    "트렌드키워드(JP)", "수요순위", "상세이미지수", "URL"
]

def update_sourcing_sheet(items: list):
    """PHASE B+C 소싱 후보 결과를 시트에 기록"""
    client = _get_sheets_client()
    if not client:
        return

    spreadsheet = _get_spreadsheet(client)
    if not spreadsheet:
        return

    ws = _get_or_create_worksheet(spreadsheet, "소싱후보")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        existing = ws.row_values(1)
        if not existing or existing[0] != SOURCING_HEADERS[0]:
            ws.update("A1", [SOURCING_HEADERS])
    except Exception:
        ws.update("A1", [SOURCING_HEADERS])

    rows = []
    for item in items:
        pi = item.get("price_info") or {}
        ci = item.get("competitor_info") or {}
        si = item.get("score_info") or {}

        rakuten_low = ci.get("rakuten", {}).get("lowest_price", "")
        kakaku_low = ci.get("kakaku", {}).get("lowest_price", "")
        qoo10_data = ci.get("qoo10")
        qoo10_low = qoo10_data.get("lowest_price", "") if qoo10_data else ""

        rows.append([
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
        ])

    if rows:
        next_row = len(ws.get_all_values()) + 1
        _ensure_sheet_rows(ws, next_row + len(rows))
        ws.update(f"A{next_row}", rows)
        logger.info(f"[시트] 소싱후보 {len(rows)}행 추가")


# ══════════════════════════════════════════════════════════════
# 3. 큐텐업로드 탭 업데이트 ★ v0.8.1
# ══════════════════════════════════════════════════════════════

def update_upload_sheet(items: list):
    """
    ★ v0.8.1: 최종 통과 상품을 공식 50컬럼 양식으로 기록
    """
    client = _get_sheets_client()
    if not client:
        return

    spreadsheet = _get_spreadsheet(client)
    if not spreadsheet:
        return

    ws = _get_or_create_worksheet(spreadsheet, "큐텐업로드", rows=1000, cols=55)

    # ★ 50컬럼 + 여유분 확보
    _ensure_sheet_cols(ws, len(OFFICIAL_HEADERS) + 2)

    # 헤더 확인/설정
    try:
        existing = ws.row_values(1)
        if not existing or existing[0] != OFFICIAL_HEADERS[0]:
            ws.update("A1", [OFFICIAL_HEADERS])
            logger.info("[시트] 큐텐업로드 헤더 설정 (공식 50컬럼)")
    except Exception:
        ws.update("A1", [OFFICIAL_HEADERS])
        logger.info("[시트] 큐텐업로드 헤더 초기화 (공식 50컬럼)")

    # 데이터 행 생성
    rows = []
    brand_matched = 0
    category_dist = {}
    weight_dist = {}

    for item in items:
        row = _build_upload_row(item)
        rows.append(row)

        # 통계
        if row[3]:  # D열: brand_number
            brand_matched += 1
        cat = row[2]  # C열: category_number
        category_dist[cat] = category_dist.get(cat, 0) + 1
        w = row[35]  # AJ열: item_weight (인덱스 35)
        weight_dist[w] = weight_dist.get(w, 0) + 1

    if rows:
        next_row = len(ws.get_all_values()) + 1
        _ensure_sheet_rows(ws, next_row + len(rows))
        ws.update(f"A{next_row}", rows)

        top5 = sorted(category_dist.items(), key=lambda x: x[1], reverse=True)[:5]
        logger.info(f"[시트] 큐텐업로드 {len(rows)}행 추가 (공식 50컬럼)")
        logger.info(f"[시트] 브랜드 매칭: {brand_matched}/{len(rows)}")
        logger.info(f"[시트] 카테고리 TOP5: {dict(top5)}")
        logger.info(f"[시트] 무게 분포: {dict(weight_dist)}")
    else:
        logger.info("[시트] 큐텐업로드 추가할 데이터 없음")


# ══════════════════════════════════════════════════════════════
# 4. 운영정보 탭 (실행 로그)
# ══════════════════════════════════════════════════════════════

def log_run_info(summary: dict):
    """파이프라인 실행 요약을 운영정보 탭에 기록"""
    client = _get_sheets_client()
    if not client:
        return

    spreadsheet = _get_spreadsheet(client)
    if not spreadsheet:
        return

    ws = _get_or_create_worksheet(spreadsheet, "운영정보")

    headers = ["실행일시", "항목", "값"]
    try:
        existing = ws.row_values(1)
        if not existing or existing[0] != headers[0]:
            ws.update("A1", [headers])
    except Exception:
        ws.update("A1", [headers])

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for key, value in summary.items():
        rows.append([timestamp, str(key), str(value)])

    if rows:
        next_row = len(ws.get_all_values()) + 1
        _ensure_sheet_rows(ws, next_row + len(rows))
        ws.update(f"A{next_row}", rows)
        logger.info(f"[시트] 운영정보 {len(rows)}행 추가")


# ══════════════════════════════════════════════════════════════
# 5. 전체 시트 업데이트 (통합)
# ══════════════════════════════════════════════════════════════

def update_all_sheets(trend_data: dict, scored_items: list,
                      final_candidates: list, run_summary: dict):
    """전체 시트 일괄 업데이트"""
    logger.info("========== Google Sheets 업데이트 시작 ==========")

    update_trend_sheet(trend_data)
    update_sourcing_sheet(scored_items)
    update_upload_sheet(final_candidates)
    log_run_info(run_summary)

    logger.info("========== Google Sheets 업데이트 완료 ==========")
    logger.info(f"  트렌드: {len(trend_data.get('sourcing_keywords', []))}행")
    logger.info(f"  소싱후보: {len(scored_items)}행")
    logger.info(f"  큐텐업로드: {len(final_candidates)}행 (공식 50컬럼)")


# ══════════════════════════════════════════════════════════════
# 직접 실행 (테스트)
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    if not QOO10_SHEET_ID:
        print("ERROR: QOO10_SHEET_ID 환경변수를 설정해주세요.")
        exit(1)

    client = _get_sheets_client()
    if client:
        spreadsheet = _get_spreadsheet(client)
        if spreadsheet:
            print(f"시트 연결 성공: {spreadsheet.title}")
            for ws in spreadsheet.worksheets():
                print(f"  탭: {ws.title} ({ws.row_count}행 × {ws.col_count}열)")
            print(f"\n큐텐업로드 헤더 수: {len(OFFICIAL_HEADERS)}컬럼 (A~AX)")
            print(f"uploader import 상태: {'성공' if _USE_UPLOADER_IMPORT else '폴백'}")
        else:
            print("시트 열기 실패")
    else:
        print("클라이언트 생성 실패")
