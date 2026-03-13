"""
uploader_qoo10.py – Qoo10 J·QSM 업로드용 엑셀 생성 & Drive 업로드 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.8

최종 통과 상품을 Qoo10 J·QSM 대량등록 엑셀 양식으로 변환하고
Google Drive에 업로드합니다.
Drive 업로드 실패 시 artifacts/ 폴더에 로컬 저장 (GitHub Artifact 다운로드용)
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
# Qoo10 카테고리 & 브랜드 매핑
# ══════════════════════════════════════════════════════════════

# 브랜드명 → Qoo10 브랜드 코드
BRAND_CODE_MAP = {
    "vt": "57851",
    "vt cosmetics": "57851",
    "브이티": "57851",
    "tirtir": "101233",
    "틸틸": "101233",
    "medicube": "57780",
    "메디큐브": "57780",
    "anua": "66607",
    "아누아": "66607",
    "d'alba": "136551",
    "달바": "136551",
    "cosrx": "55360",
    "코스알엑스": "55360",
    "innisfree": "20802",
    "이니스프리": "20802",
    "laneige": "25658",
    "라네즈": "25658",
    "sulwhasoo": "27508",
    "설화수": "27508",
}

# 키워드 → Qoo10 소카테고리 코드 (우선순위: 위에서부터 매칭)
KEYWORD_CATEGORY_MAP = [
    # ── 스킨케어 ──
    (["토너", "스킨", "화장수", "toner"], "320001619"),
    (["토너패드", "패드", "pad"], "320001620"),
    (["아이크림", "아이젤", "eye cream"], "320001622"),
    (["에센스", "세럼", "앰플", "serum", "ampoule", "essence"], "320001623"),
    (["페이스오일", "face oil"], "320001624"),
    (["올인원", "all in one"], "320001625"),
    (["보습젤", "수분젤", "gel"], "320001626"),
    (["크림", "로션", "cream", "lotion", "moisturizer"], "320001621"),

    # ── 팩/마스크 ──
    (["마스크팩", "시트마스크", "sheet mask", "mask pack"], "320001628"),
    (["워시오프", "wash off", "씻어내는"], "320001627"),
    (["모공팩", "pore"], "320001629"),
    (["아이패치", "eye patch"], "320001630"),
    (["코팩", "nose"], "320001631"),
    (["립팩", "lip pack", "lip mask"], "320001632"),

    # ── 미스트 ──
    (["픽서", "fixer", "세팅"], "320001633"),
    (["미스트", "mist"], "320001634"),

    # ── 각질/마사지 ──
    (["필링", "고마주", "peeling", "각질"], "320001635"),
    (["마사지크림", "massage cream"], "320001636"),
    (["마사지오일", "massage oil"], "320001637"),

    # ── 세안/클렌징 ──
    (["클렌징폼", "세안폼", "cleansing foam"], "320001639"),
    (["세안비누", "세안 비누", "cleansing soap"], "320001640"),
    (["세안파우더", "cleansing powder"], "320001641"),
    (["클렌징오일", "cleansing oil"], "320001643"),
    (["클렌징밀크", "cleansing milk"], "320001644"),
    (["클렌징크림", "cleansing cream"], "320001645"),
    (["클렌징워터", "미셀라", "cleansing water", "micellar"], "320001646"),
    (["클렌징젤", "cleansing gel"], "320001648"),
    (["클렌징밤", "cleansing balm"], "320001649"),
    (["포인트리무버", "리무버", "remover"], "320001650"),
    (["클렌징시트", "클렌징티슈", "cleansing sheet"], "320001651"),

    # ── 베이스 메이크업 ──
    (["쿠션", "cushion"], "320001663"),
    (["리퀴드파운데이션", "liquid foundation"], "320001660"),
    (["파우더파운데이션", "powder foundation"], "320001661"),
    (["크림파운데이션", "cream foundation"], "320001662"),
    (["스틱파운데이션", "stick foundation"], "320001664"),
    (["파운데이션", "foundation"], "320001660"),
    (["루스파우더", "loose powder"], "320001665"),
    (["프레스트파우더", "pressed powder"], "320001666"),
    (["컨실러", "concealer"], "320001655"),
    (["하이라이터", "하이라이트", "highlighter"], "320001667"),
    (["쉐이딩", "shading", "컨투어"], "320001670"),
    (["bb크림", "bb cream", "비비크림"], "320001673"),
    (["cc크림", "cc cream", "씨씨크림"], "320001674"),
    (["메이크업베이스", "프라이머", "primer", "makeup base"], "320001676"),

    # ── 포인트 메이크업: 아이 ──
    (["아이브로우", "eyebrow", "눈썹"], "320001683"),
    (["아이섀도우팔레트", "eyeshadow palette", "섀도팔레트"], "320001698"),
    (["아이섀도우", "eyeshadow", "섀도우"], "320001695"),
    (["아이라이너", "eyeliner"], "320001701"),
    (["마스카라", "mascara"], "320001705"),
    (["속눈썹", "인조속눈썹", "false lash"], "320001707"),

    # ── 포인트 메이크업: 립 ──
    (["립스틱", "lipstick"], "320001710"),
    (["틴트", "tint"], "320001714"),
    (["립글로스", "lip gloss", "글로스"], "320001715"),
    (["립밤", "lip balm"], "320001716"),
    (["립라이너", "lip liner"], "320001713"),

    # ── 포인트 메이크업: 치크 ──
    (["블러셔", "블러쉬", "치크", "blush", "blusher"], "320001717"),

    # ── 자외선 차단 ──
    (["선크림", "선로션", "자외선", "sunscreen", "sun cream", "spf", "uv"], "320001741"),
    (["선스틱", "sun stick"], "320001742"),
    (["선쿠션", "sun cushion"], "320001743"),
    (["선젤", "sun gel"], "320001744"),
    (["선스프레이", "sun spray"], "320001746"),

    # ── 바디케어 ──
    (["바디워시", "body wash", "바디클렌저"], "320001753"),
    (["바디스크럽", "body scrub"], "320001754"),
    (["바디오일", "body oil"], "320001755"),
    (["바디크림", "바디로션", "body cream", "body lotion"], "320001756"),
    (["바디미스트", "body mist"], "320001760"),
    (["데오드란트", "deodorant"], "320001752"),

    # ── 핸드/풋 ──
    (["핸드크림", "hand cream"], "320001763"),
    (["풋크림", "foot cream", "발크림"], "320001767"),

    # ── 헤어케어 ──
    (["샴푸", "shampoo"], "320001775"),
    (["컨디셔너", "conditioner", "린스"], "320001777"),
    (["헤어오일", "hair oil"], "320001778"),
    (["트리트먼트", "헤어팩", "treatment", "hair pack"], "320001780"),
    (["헤어에센스", "hair essence"], "320001781"),
    (["염색", "헤어컬러", "hair color", "hair dye"], "320001793"),
    (["왁스", "젤", "스타일링", "wax", "styling"], "320001784"),

    # ── 네일 ──
    (["매니큐어", "네일폴리시", "nail polish"], "320001808"),
    (["젤네일", "gel nail", "컬러젤"], "320001809"),
    (["네일스티커", "nail sticker"], "320001806"),
    (["네일팁", "nail tip"], "320001805"),

    # ── 향수 ──
    (["향수", "퍼퓸", "오드뚜왈렛", "perfume", "eau de"], "320001829"),

    # ── 남성 ──
    (["남성올인원", "맨즈올인원", "men all in one"], "320001838"),
    (["남성토너", "men toner"], "320001834"),
    (["남성크림", "men cream"], "320001835"),
    (["남성에센스", "men essence"], "320001836"),
    (["쉐이빙", "면도", "shaving"], "320001844"),

    # ── 메이크업 소품 ──
    (["브러쉬", "브러시", "brush"], "320001725"),
    (["퍼프", "스펀지", "puff", "sponge"], "320001730"),
    (["뷰러", "eyelash curler"], "320001727"),

    # ── 다이어트/건강 ──
    (["콜라겐", "collagen"], "320002809"),
    (["프로틴", "protein"], "320002766"),
    (["비타민", "vitamin"], "320002747"),
    (["유산균", "프로바이오틱스", "probiotics"], "320002771"),
]

# KJ9603 중분류 → Qoo10 기본값 (키워드 매칭 실패 시)
DEFAULT_CATEGORY_MAP = {
    "680": "320001621",   # SkinCare → 로션・크림
    "688": "320001663",   # Makeup → 쿠션 파운데이션
    "695": "320001639",   # Cleansing → 클렌징 폼
    "700": "320001756",   # BodyCare → 바디 크림・로션
    "706": "320001775",   # HairCare → 샴푸
    "712": "320001741",   # Suncare → 선크림・로션
    "718": "320001838",   # MensCare → 올인원 화장품
    "724": "320001808",   # Nail → 매니큐어
    "730": "320001725",   # Tools → 메이크업 브러쉬
    "736": "320001829",   # Fragrance → 향수
    "742": "320002809",   # Diet/Health → 콜라겐
    "748": "320000473",   # BabyKids → 베이비 로션・크림
}


def _match_qoo10_category(item):
    """상품명 키워드 기반 Qoo10 소카테고리 코드 매칭"""
    name = (item.get("name", "") + " " + item.get("name_jp", "")).lower()

    for keywords, code in KEYWORD_CATEGORY_MAP:
        for kw in keywords:
            if kw.lower() in name:
                return code

    # 키워드 매칭 실패 → KJ9603 카테고리 기본값
    cat_id = str(item.get("category_id", "") or item.get("kj_category", ""))
    return DEFAULT_CATEGORY_MAP.get(cat_id, "320001621")


def _match_brand_code(item):
    """브랜드명 → Qoo10 브랜드 코드 매칭"""
    brand = (item.get("brand", "")).lower().strip()
    name = (item.get("name", "")).lower()

    # 브랜드 필드에서 매칭
    if brand in BRAND_CODE_MAP:
        return BRAND_CODE_MAP[brand]

    # 상품명에서 브랜드 키워드 매칭
    for kw, code in BRAND_CODE_MAP.items():
        if kw in name:
            return code

    return ""  # 매칭 실패 → 빈칸 (수동 입력)


# ══════════════════════════════════════════════════════════════
# 1. Qoo10 엑셀 생성
# ══════════════════════════════════════════════════════════════

def generate_qoo10_excel(items: list) -> BytesIO:
    """
    Qoo10 J·QSM 대량등록용 엑셀 파일 생성 (공식 양식 준수)
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

    # ── Qoo10 공식 대량등록 헤더 (A~AF열) ──
    headers = [
        "item_number",                # A
        "seller_unique_item_id",      # B
        "category_number",            # C
        "brand_number",               # D
        "item_name",                  # E
        "item_promotion_name",        # F
        "item_status_Y/N/D",          # G
        "end_date",                   # H
        "price_yen",                  # I
        "retail_price_yen",           # J
        "quantity",                   # K
        "option_info",                # L
        "additional_option_info",     # M
        "additional_option_text",     # N
        "image_main_url",             # O
        "image_other_url",            # P
        "video_url",                  # Q
        "image_option_info",          # R
        "image_additional_option_info",  # S
        "header_html",                # T
        "footer_html",                # U
        "item_description",           # V
        "Shipping_number",            # W
        "option_number",              # X
        "available_shipping_date",    # Y
        "desired_shipping_date",      # Z
        "search_keyword",             # AA
        "item_condition_type",        # AB
        "origin_type",                # AC
        "origin_region_id",           # AD
        "origin_country_id",          # AE
        "origin_others",              # AF
    ]

    header_font = Font(bold=True, size=10)
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    # ── 데이터 행 ──
    cat_stats = {}  # 카테고리 매핑 통계
    brand_matched = 0

    for row_idx, item in enumerate(items, 2):
        # 카테고리 매핑
        qoo10_cat = _match_qoo10_category(item)
        cat_stats[qoo10_cat] = cat_stats.get(qoo10_cat, 0) + 1

        # 브랜드 매핑
        qoo10_brand = _match_brand_code(item)
        if qoo10_brand:
            brand_matched += 1

        # 이미지
        thumbnail = item.get("thumbnail", "") or item.get("image_url", "")
        detail_images = item.get("detail_images", [])
        other_images = "||".join(detail_images[:20]) if detail_images else ""

        # 옵션 (Qoo10 형식)
        detail = item.get("detail_info", {})
        opt_name = detail.get("option1_name", "")
        opt_values = detail.get("option1_values", [])
        option_str = ""
        if opt_name and opt_values:
            sell_price = item.get("sell_price_jpy", 0)
            option_parts = [f"{v}^{sell_price}^99^0^0" for v in opt_values]
            option_str = f"{opt_name}:#{'$$'.join(option_parts)}"

        # 검색 키워드
        name_jp = item.get("name_jp", item.get("name", ""))
        brand = item.get("brand", "")
        search_parts = [kw.strip() for kw in name_jp.split() if kw.strip()][:8]
        if brand and brand not in search_parts:
            search_parts.insert(0, brand)
        search_parts = search_parts[:10]
        search_keyword_str = ",".join(search_parts) if search_parts else name_jp[:30]

        # 참고가격 (소비자가 또는 판매가의 1.3배)
        sell_price = item.get("sell_price_jpy", 0)
        retail_price = item.get("consumer_price_jpy", 0)
        if retail_price <= sell_price:
            retail_price = int(sell_price * 1.3)

        # 행 데이터 (A~AF)
        row_data = [
            "",                                                    # A: item_number
            f"KJ{item.get('product_id', '')}",                    # B: seller_unique_item_id
            qoo10_cat,                                             # C: category_number
            qoo10_brand,                                           # D: brand_number
            name_jp[:50],                                          # E: item_name
            name_jp[:20],                                          # F: item_promotion_name
            "Y",                                                   # G: item_status
            "",                                                    # H: end_date
            sell_price,                                            # I: price_yen
            retail_price,                                          # J: retail_price_yen
            99,                                                    # K: quantity
            option_str,                                            # L: option_info
            "",                                                    # M: additional_option_info
            "",                                                    # N: additional_option_text
            thumbnail,                                             # O: image_main_url
            other_images,                                          # P: image_other_url
            "",                                                    # Q: video_url
            "",                                                    # R: image_option_info
            "",                                                    # S: image_additional_option_info
            "",                                                    # T: header_html
            "",                                                    # U: footer_html
            item.get("detail_html", ""),                          # V: item_description
            QOO10_KSE_SHIPPING_CODE,                              # W: Shipping_number
            "",                                                    # X: option_number
            "一般発送",                                             # Y: available_shipping_date
            "7",                                                   # Z: desired_shipping_date
            search_keyword_str,                                    # AA: search_keyword
            "1",                                                   # AB: item_condition_type (新品)
            "2",                                                   # AC: origin_type (海外)
            "",                                                    # AD: origin_region_id
            "KR",                                                  # AE: origin_country_id
            "",                                                    # AF: origin_others
        ]

        for col, value in enumerate(row_data, 1):
            ws.cell(row=row_idx, column=col, value=value)

    # ── 열 너비 조정 ──
    col_widths = {
        "A": 12, "B": 18, "C": 15, "D": 12, "E": 50,
        "F": 25, "G": 8, "H": 12, "I": 12, "J": 12,
        "K": 8, "L": 40, "M": 15, "N": 15, "O": 50,
        "P": 50, "Q": 20, "R": 20, "S": 20, "T": 20,
        "U": 20, "V": 30, "W": 15, "X": 15, "Y": 15,
        "Z": 8, "AA": 40, "AB": 8, "AC": 8, "AD": 10,
        "AE": 8, "AF": 15,
    }
    for col_letter, width in col_widths.items():
        ws.column_dimensions[col_letter].width = width

    # BytesIO로 저장
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    logger.info(f"[엑셀] Qoo10 업로드 파일 생성: {len(items)}건")
    logger.info(f"[엑셀] 브랜드 매칭: {brand_matched}/{len(items)}건")
    logger.info(f"[엑셀] 카테고리 분포: {dict(sorted(cat_stats.items(), key=lambda x: -x[1])[:5])}")

    return output


# ══════════════════════════════════════════════════════════════
# 2. Google Drive 업로드 (★ v0.8 수정: 로컬 백업 + return 버그 수정)
# ══════════════════════════════════════════════════════════════

def upload_to_drive(file_bytes: BytesIO, filename: str = None) -> str:
    """
    엑셀 파일을 Google Drive에 업로드
    실패 시 artifacts/ 폴더에 로컬 저장 (GitHub Artifact 다운로드용)

    Args:
        file_bytes: BytesIO 객체
        filename: 파일명 (None이면 자동 생성)
    Returns:
        업로드된 파일 URL / "LOCAL:경로" / 빈 문자열
    """
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"qoo10_upload_{timestamp}.xlsx"

    # ★ ① 항상 로컬 백업 먼저 (artifacts/)
    artifact_dir = os.path.join(os.getcwd(), "artifacts")
    os.makedirs(artifact_dir, exist_ok=True)
    local_path = os.path.join(artifact_dir, filename)

    file_bytes.seek(0)
    with open(local_path, "wb") as f:
        f.write(file_bytes.read())
    logger.info(f"[로컬] artifacts 백업 완료: {local_path}")

    # ★ ② Drive 업로드 시도
    if not QOO10_DRIVE_FOLDER_ID:
        logger.warning("QOO10_DRIVE_FOLDER_ID 미설정 → 로컬 백업만 사용")
        return f"LOCAL:{local_path}"

    try:
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaIoBaseUpload
        from google.oauth2 import service_account

        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if not sa_json:
            logger.warning("GOOGLE_SERVICE_ACCOUNT_JSON 미설정 → 로컬 백업만 사용")
            return f"LOCAL:{local_path}"

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

        # ★ BytesIO 위치 리셋 (로컬 저장 후 포인터가 끝에 있으므로)
        file_bytes.seek(0)

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

        web_link = file.get("webViewLink", "")
        file_id = file.get("id", "")
        logger.info(f"[Drive] 업로드 성공: {web_link}")

        # 소유권 이전 시도
        try:
            service.permissions().create(
                fileId=file_id,
                body={
                    "type": "user",
                    "role": "owner",
                    "emailAddress": "soccercamp.beta@gmail.com",
                },
                transferOwnership=True,
                supportsAllDrives=True,
            ).execute()
            logger.info("[Drive] 소유권 이전 완료")
        except Exception as perm_err:
            logger.warning(f"[Drive] 소유권 이전 실패 (무시): {perm_err}")

        return web_link  # ★ 기존 v0.7에서 빠져있던 return 추가

    except ImportError:
        logger.warning("google-api-python-client 미설치 → 로컬 백업만 사용")
        return f"LOCAL:{local_path}"
    except Exception as e:
        # ★ error → warning 으로 변경, 파이프라인 중단 방지
        logger.warning(f"[Drive] 업로드 실패 (로컬 백업 사용): {e}")
        return f"LOCAL:{local_path}"


# ══════════════════════════════════════════════════════════════
# 3. 통합 실행 (엑셀 생성 + 업로드)  ★ v0.8 수정
# ══════════════════════════════════════════════════════════════

def generate_and_upload(items: list) -> dict:
    """
    최종 통과 상품 → 엑셀 생성 → Drive 업로드 (실패 시 로컬 저장)
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"qoo10_upload_{timestamp}.xlsx"

    # 엑셀 생성
    excel_bytes = generate_qoo10_excel(items)
    if excel_bytes is None:
        return {"item_count": 0, "filename": "", "drive_url": "", "success": False}

    # Drive 업로드 (실패 시 로컬 저장)
    drive_url = upload_to_drive(excel_bytes, filename)

    # ★ LOCAL: 접두어면 Drive 실패했지만 로컬 백업은 성공
    is_local = drive_url.startswith("LOCAL:") if drive_url else False

    result = {
        "item_count": len(items),
        "filename":   filename,
        "drive_url":  drive_url,
        "success":    bool(drive_url),   # ★ 로컬 저장도 성공으로 처리
        "is_local":   is_local,
    }

    if drive_url and not is_local:
        logger.info(f"[업로드] Drive 성공: {len(items)}건 → {drive_url}")
    elif is_local:
        logger.info(f"[업로드] 로컬 저장: {len(items)}건 → {drive_url}")
        logger.info("[업로드] GitHub Actions Artifacts 탭에서 다운로드 가능")
    else:
        logger.error("[업로드] 실패: 엑셀 생성 또는 저장 에러")

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
