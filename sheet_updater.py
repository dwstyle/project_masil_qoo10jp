"""
sheet_updater.py – Google Sheets 업데이트 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.7

시트 탭 구조:
  1. 트렌드분석 – PHASE A 결과 (라쿠텐·구글 트렌드)
  2. 소싱후보  – PHASE B+C 결과 (상품·가격·경쟁가·점수)
  3. 큐텐업로드 – 최종 통과 상품 (업로드 대상)
  4. 카테고리매핑 – KJ9603 ↔ Qoo10 카테고리
  5. 운영정보  – 실행 로그, 환율, 배송비 변동 기록
"""

import os
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────────────
QOO10_SHEET_ID = os.environ.get("QOO10_SHEET_ID", "")


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
        logger.info(f"워크시트 '{title}' 생성")
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


# ══════════════════════════════════════════════════════════════
# 1. 트렌드분석 탭 업데이트
# ══════════════════════════════════════════════════════════════

TREND_HEADERS = [
    "수집일시", "소스", "순위", "상품명(JP)", "브랜드", "카테고리",
    "가격(JPY)", "리뷰수", "평점", "키워드(JP)", "키워드(KR)",
    "트렌드점수", "KJ9603매칭", "매칭ID"
]

def update_trend_sheet(trend_data: dict):
    """
    PHASE A 트렌드 분석 결과를 시트에 기록
    Args:
        trend_data: trend_analyzer.run_trend_analysis_combined() 반환값
    """
    client = _get_sheets_client()
    if not client:
        return

    spreadsheet = _get_spreadsheet(client)
    if not spreadsheet:
        return

    ws = _get_or_create_worksheet(spreadsheet, "트렌드분석")
    timestamp = trend_data.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # 헤더 확인/설정
    try:
        existing = ws.row_values(1)
        if not existing or existing[0] != TREND_HEADERS[0]:
            ws.update("A1", [TREND_HEADERS])
            logger.info("[시트] 트렌드분석 헤더 설정")
    except Exception:
        ws.update("A1", [TREND_HEADERS])

    # 소싱 키워드 데이터 추가
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
    """
    PHASE B+C 소싱 후보 결과를 시트에 기록
    Args:
        items: 스코어링 완료된 상품 리스트
    """
    client = _get_sheets_client()
    if not client:
        return

    spreadsheet = _get_spreadsheet(client)
    if not spreadsheet:
        return

    ws = _get_or_create_worksheet(spreadsheet, "소싱후보")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 헤더
    try:
        existing = ws.row_values(1)
        if not existing or existing[0] != SOURCING_HEADERS[0]:
            ws.update("A1", [SOURCING_HEADERS])
    except Exception:
        ws.update("A1", [SOURCING_HEADERS])

    # 데이터 행 생성
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
        ws.update(f"A{next_row}", rows)
        logger.info(f"[시트] 소싱후보 {len(rows)}행 추가")


# ══════════════════════════════════════════════════════════════
# 3. 큐텐업로드 탭 업데이트
# ══════════════════════════════════════════════════════════════

UPLOAD_HEADERS = [
    "수집일시", "상품ID", "상품명(JP)", "판매가(JPY)", "마진율",
    "등급", "최종점수", "카테고리(JP)", "브랜드",
    "상세이미지수", "HTML생성", "업로드상태"
]

def update_upload_sheet(items: list):
    """
    최종 통과 상품을 큐텐업로드 탭에 기록
    Args:
        items: pass_final == True인 상품 리스트
    """
    client = _get_sheets_client()
    if not client:
        return

    spreadsheet = _get_spreadsheet(client)
    if not spreadsheet:
        return

    ws = _get_or_create_worksheet(spreadsheet, "큐텐업로드")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        existing = ws.row_values(1)
        if not existing or existing[0] != UPLOAD_HEADERS[0]:
            ws.update("A1", [UPLOAD_HEADERS])
    except Exception:
        ws.update("A1", [UPLOAD_HEADERS])

    rows = []
    for item in items:
        rows.append([
            timestamp,
            item.get("item_id", ""),
            item.get("name_jp", ""),
            item.get("sell_price_jpy", ""),
            f"{item.get('margin_rate', 0) * 100:.1f}%",
            item.get("grade", ""),
            item.get("final_score", ""),
            item.get("category_jp", ""),
            item.get("brand", ""),
            len(item.get("detail_images", [])),
            "✅" if item.get("detail_html") else "❌",
            "대기",
        ])

    if rows:
        next_row = len(ws.get_all_values()) + 1
        ws.update(f"A{next_row}", rows)
        logger.info(f"[시트] 큐텐업로드 {len(rows)}행 추가")


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
        ws.update(f"A{next_row}", rows)
        logger.info(f"[시트] 운영정보 {len(rows)}행 추가")


# ══════════════════════════════════════════════════════════════
# 5. 전체 시트 업데이트 (통합)
# ══════════════════════════════════════════════════════════════

def update_all_sheets(trend_data: dict, scored_items: list, final_candidates: list, run_summary: dict):
    """
    전체 시트 일괄 업데이트
    Args:
        trend_data: PHASE A 결과
        scored_items: 스코어링 완료된 전체 상품
        final_candidates: 최종 통과 상품
        run_summary: 실행 요약 정보
    """
    logger.info("========== Google Sheets 업데이트 시작 ==========")

    update_trend_sheet(trend_data)
    update_sourcing_sheet(scored_items)
    update_upload_sheet(final_candidates)
    log_run_info(run_summary)

    logger.info(f"========== Google Sheets 업데이트 완료 ==========")
    logger.info(f"  트렌드: {len(trend_data.get('sourcing_keywords', []))}행")
    logger.info(f"  소싱후보: {len(scored_items)}행")
    logger.info(f"  큐텐업로드: {len(final_candidates)}행")


# ══════════════════════════════════════════════════════════════
# 직접 실행 (테스트)
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not QOO10_SHEET_ID:
        print("ERROR: QOO10_SHEET_ID 환경변수를 설정해주세요.")
        exit(1)

    # 연결 테스트
    client = _get_sheets_client()
    if client:
        spreadsheet = _get_spreadsheet(client)
        if spreadsheet:
            print(f"시트 연결 성공: {spreadsheet.title}")
            worksheets = spreadsheet.worksheets()
            for ws in worksheets:
                print(f"  탭: {ws.title}")
        else:
            print("시트 열기 실패")
    else:
        print("클라이언트 생성 실패")
