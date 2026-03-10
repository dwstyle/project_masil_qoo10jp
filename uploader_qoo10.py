"""
uploader_qoo10.py – Qoo10 J·QSM 업로드용 엑셀 생성 & Drive 업로드 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.7

최종 통과 상품을 Qoo10 J·QSM 대량등록 엑셀 양식으로 변환하고
Google Drive에 업로드합니다.
"""

import os
import json
import logging
from datetime import datetime
from io import BytesIO

logger = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────────────
QOO10_DRIVE_FOLDER_ID    = os.environ.get("QOO10_DRIVE_FOLDER_ID", "")
QOO10_KSE_SHIPPING_CODE  = os.environ.get("QOO10_KSE_SHIPPING_CODE", "813137")


# ══════════════════════════════════════════════════════════════
# 1. Qoo10 엑셀 생성
# ══════════════════════════════════════════════════════════════

def generate_qoo10_excel(items: list) -> BytesIO:
    """
    Qoo10 J·QSM 대량등록용 엑셀 파일 생성
    Args:
        items: 최종 통과 상품 리스트
    Returns:
        BytesIO 객체 (엑셀 파일 바이너리)
    """
    try:
        import openpyxl
        from openpyxl.styles import Font, Alignment
    except ImportError:
        logger.error("openpyxl 미설치 – pip install openpyxl")
        return None

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Qoo10_Upload"

    # ── 헤더 ──────────────────────────────────────
    headers = [
        "商品名",              # A: 상품명 (일본어)
        "販売価格",            # B: 판매가 (JPY)
        "在庫数",              # C: 재고수
        "商品状態",            # D: 상품 상태
        "原産国",              # E: 원산지
        "配送方法コード",       # F: 배송비 코드
        "ブランド",            # G: 브랜드
        "商品説明(HTML)",      # H: 상세 설명 HTML
        "メイン画像URL",       # I: 메인 이미지
        "追加画像URL1",        # J: 추가 이미지1
        "追加画像URL2",        # K: 추가 이미지2
        "追加画像URL3",        # L: 추가 이미지3
        "オプション1名",       # M: 옵션1 이름
        "オプション1値",       # N: 옵션1 값
        "オプション2名",       # O: 옵션2 이름
        "オプション2値",       # P: 옵션2 값
        "KJ9603_ID",          # Q: 내부 참조용
        "KJ9603_URL",         # R: 내부 참조용
        "供給価格(KRW)",       # S: 내부 참조용
        "マージン率",          # T: 내부 참조용
        "最終スコア",          # U: 내부 참조용
        "等級",               # V: 내부 참조용
    ]

    # 헤더 스타일링
    header_font = Font(bold=True, size=11)
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    # ── 데이터 행 ─────────────────────────────────
    for row_idx, item in enumerate(items, 2):
        # 옵션 정보
        detail = item.get("detail_info", {})
        opt1_name = detail.get("option1_name", "")
        opt1_values = ", ".join(detail.get("option1_values", []))
        opt2_name = detail.get("option2_name", "")
        opt2_values = ", ".join(detail.get("option2_values", []))

        # 이미지
        thumbnail = item.get("thumbnail", "")
        detail_images = item.get("detail_images", [])
        img1 = detail_images[0] if len(detail_images) > 0 else ""
        img2 = detail_images[1] if len(detail_images) > 1 else ""
        img3 = detail_images[2] if len(detail_images) > 2 else ""

        # 행 데이터
        row_data = [
            item.get("name_jp", item.get("name", "")),         # A: 商品名
            item.get("sell_price_jpy", 0),                      # B: 販売価格
            99,                                                  # C: 在庫数
            "新品",                                              # D: 商品状態
            "韓国",                                              # E: 原産国
            QOO10_KSE_SHIPPING_CODE,                            # F: 配送方法コード
            item.get("brand", ""),                               # G: ブランド
            item.get("detail_html", ""),                         # H: 商品説明
            thumbnail,                                           # I: メイン画像
            img1,                                                # J: 追加画像1
            img2,                                                # K: 追加画像2
            img3,                                                # L: 追加画像3
            opt1_name,                                           # M: オプション1名
            opt1_values,                                         # N: オプション1値
            opt2_name,                                           # O: オプション2名
            opt2_values,                                         # P: オプション2値
            item.get("item_id", ""),                             # Q: KJ9603_ID
            item.get("url", ""),                                 # R: KJ9603_URL
            item.get("supply_price", 0),                         # S: 供給価格
            f"{item.get('margin_rate', 0) * 100:.1f}%",         # T: マージン率
            item.get("final_score", 0),                          # U: 最終スコア
            item.get("grade", ""),                               # V: 等級
        ]

        for col, value in enumerate(row_data, 1):
            ws.cell(row=row_idx, column=col, value=value)

    # ── 열 너비 조정 ──────────────────────────────
    col_widths = {
        "A": 50, "B": 12, "C": 8, "D": 10, "E": 8,
        "F": 15, "G": 20, "H": 30, "I": 40, "J": 40,
        "K": 40, "L": 40, "M": 15, "N": 30, "O": 15,
        "P": 30, "Q": 12, "R": 40, "S": 12, "T": 10,
        "U": 10, "V": 8,
    }
    for col_letter, width in col_widths.items():
        ws.column_dimensions[col_letter].width = width

    # BytesIO로 저장
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    logger.info(f"[엑셀] Qoo10 업로드 파일 생성: {len(items)}건")
    return output


# ══════════════════════════════════════════════════════════════
# 2. Google Drive 업로드
# ══════════════════════════════════════════════════════════════

def upload_to_drive(file_bytes: BytesIO, filename: str = None) -> str:
    """
    엑셀 파일을 Google Drive에 업로드
    Args:
        file_bytes: BytesIO 객체
        filename: 파일명 (None이면 자동 생성)
    Returns:
        업로드된 파일 URL (실패 시 빈 문자열)
    """
    if not QOO10_DRIVE_FOLDER_ID:
        logger.error("QOO10_DRIVE_FOLDER_ID 미설정")
        return ""

    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"qoo10_upload_{timestamp}.xlsx"

    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseUpload
        from google.oauth2 import service_account

        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not sa_json:
            logger.error("GOOGLE_SERVICE_ACCOUNT_JSON 미설정")
            return ""

        sa_info = json.loads(sa_json)
        credentials = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/drive"]
        )

        service = build("drive", "v3", credentials=credentials)

        file_metadata = {
            "name": filename,
            "parents": [QOO10_DRIVE_FOLDER_ID],
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }

        media = MediaIoBaseUpload(
            file_bytes,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            resumable=True,
        )

        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, webViewLink",
            supportsAllDrives=True,
        ).execute()

        # 소유자에게 권한 부여
        try:
            service.permissions().create(
                fileId=file.get("id"),
                body={
                    "type": "user",
                    "role": "writer",
                    "emailAddress": "dwstyle80@gmail.com",
                },
                transferOwnership=False,
            ).execute()
        except Exception as perm_err:
            logger.warning(f"[Drive] 권한 부여 실패 (무시): {perm_err}")

        file_id = file.get("id", "")
        file_url = file.get("webViewLink", f"https://drive.google.com/file/d/{file_id}/view")

        logger.info(f"[Drive] 업로드 완료: {filename} → {file_url}")
        return file_url

    except ImportError:
        logger.error("google-api-python-client 미설치 – pip install google-api-python-client")
        return ""
    except Exception as e:
        logger.error(f"[Drive] 업로드 실패: {e}")
        return ""


# ══════════════════════════════════════════════════════════════
# 3. 통합 실행 (엑셀 생성 + 업로드)
# ══════════════════════════════════════════════════════════════

def generate_and_upload(items: list) -> dict:
    """
    최종 통과 상품 → 엑셀 생성 → Drive 업로드
    Args:
        items: 최종 통과 상품 리스트
    Returns:
        {
            "item_count": 50,
            "filename": "qoo10_upload_20260304_143000.xlsx",
            "drive_url": "https://drive.google.com/...",
            "success": True,
        }
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"qoo10_upload_{timestamp}.xlsx"

    # 엑셀 생성
    excel_bytes = generate_qoo10_excel(items)
    if excel_bytes is None:
        return {"item_count": 0, "filename": "", "drive_url": "", "success": False}

    # Drive 업로드
    drive_url = upload_to_drive(excel_bytes, filename)

    result = {
        "item_count": len(items),
        "filename":   filename,
        "drive_url":  drive_url,
        "success":    bool(drive_url),
    }

    if drive_url:
        logger.info(f"[업로드] 성공: {len(items)}건 → {filename}")
    else:
        logger.error(f"[업로드] 실패: Drive 업로드 에러")

    return result


# ══════════════════════════════════════════════════════════════
# 직접 실행 (테스트)
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # 테스트: 더미 데이터로 엑셀 생성
    test_items = [
        {
            "name": "어성초 77% 수딩 토너",
            "name_jp": "【ANUA】ドクダミ77% スージングトナー 250ml",
            "brand": "ANUA",
            "item_id": "12345",
            "url": "https://kj9603.example.com/mitem.php?item=12345",
            "supply_price": 8000,
            "sell_price_jpy": 2600,
            "margin_rate": 0.217,
            "final_score": 82,
            "grade": "A",
            "thumbnail": "https://kmclubb2b.com/thumb1.jpg",
            "detail_images": [
                "https://kmclubb2b.com/detail1.jpg",
                "https://kmclubb2b.com/detail2.jpg",
            ],
            "detail_html": "<div>テスト商品説明</div>",
            "detail_info": {
                "option1_name": "容量",
                "option1_values": ["250ml", "500ml"],
                "option2_name": "",
                "option2_values": [],
            },
        },
    ]

    excel = generate_qoo10_excel(test_items)
    if excel:
        # 로컬 저장 테스트
        with open("test_qoo10_upload.xlsx", "wb") as f:
            f.write(excel.read())
        print("테스트 엑셀 생성 완료: test_qoo10_upload.xlsx")
    else:
        print("엑셀 생성 실패")
