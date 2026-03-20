"""
uploader_qoo10.py – Qoo10 J·QSM 업로드용 엑셀 생성 & Drive 업로드 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.9.1

★ v0.9 → v0.9.1 변경사항:
  1. 공식 양식 50컬럼 대응 (H: start_date, L: taxrate 추가)
  2. 추가이미지 구분자 || → $$ (Qoo10 공식)
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
from io import BytesIO

logger = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────────────
QOO10_DRIVE_FOLDER_ID = os.environ.get("QOO10_DRIVE_FOLDER_ID", "")
QOO10_KSE_SHIPPING_CODE = os.environ.get("QOO10_KSE_SHIPPING_CODE", "813137")


# ══════════════════════════════════════════════════════════════
# Qoo10 카테고리 & 브랜드 매핑
# ══════════════════════════════════════════════════════════════

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

DEFAULT_CATEGORY_MAP = {
    "680": "320001621",
    "688": "320001663",
    "695": "320001639",
    "700": "320001756",
    "706": "320001775",
    "712": "320001741",
    "718": "320001838",
    "724": "320001808",
    "730": "320001725",
    "736": "320001829",
    "742": "320002809",
    "748": "320000473",
}

CATEGORY_WEIGHT_MAP = {
    # 립메이크업 — 0.10kg
    "320001710": 0.10, "320001714": 0.10, "320001715": 0.10,
    "320001716": 0.10, "320001713": 0.10,

    # 소형 포인트메이크업/스킨케어 — 0.25kg
    "320001623": 0.25, "320001622": 0.25, "320001624": 0.25,
    "320001625": 0.25, "320001633": 0.25, "320001634": 0.25,
    "320001655": 0.25, "320001663": 0.25, "320001667": 0.25,
    "320001670": 0.25, "320001673": 0.25, "320001674": 0.25,
    "320001676": 0.25, "320001683": 0.25, "320001695": 0.25,
    "320001698": 0.25, "320001701": 0.25, "320001705": 0.25,
    "320001707": 0.25, "320001717": 0.25, "320001725": 0.25,
    "320001727": 0.25, "320001730": 0.25, "320001741": 0.25,
    "320001742": 0.25, "320001743": 0.25, "320001744": 0.25,
    "320001746": 0.25, "320001763": 0.25, "320001767": 0.25,
    "320001806": 0.25, "320001805": 0.25, "320001808": 0.25,
    "320001809": 0.25, "320001829": 0.25, "320001834": 0.25,
    "320001835": 0.25, "320001836": 0.25, "320001838": 0.25,
    "320001844": 0.25,

    # 토너/로션/클렌징/크림/팩 — 0.50kg
    "320001619": 0.50, "320001620": 0.50, "320001621": 0.50,
    "320001626": 0.50, "320001627": 0.50, "320001628": 0.50,
    "320001629": 0.50, "320001630": 0.50, "320001631": 0.50,
    "320001632": 0.50, "320001635": 0.50, "320001636": 0.50,
    "320001637": 0.50, "320001639": 0.50, "320001640": 0.50,
    "320001641": 0.50, "320001643": 0.50, "320001644": 0.50,
    "320001645": 0.50, "320001646": 0.50, "320001648": 0.50,
    "320001649": 0.50, "320001650": 0.50, "320001651": 0.50,
    "320001660": 0.50, "320001661": 0.50, "320001662": 0.50,
    "320001664": 0.50, "320001665": 0.50, "320001666": 0.50,

    # 헤어/바디 — 0.75kg
    "320001753": 0.75, "320001754": 0.75, "320001755": 0.75,
    "320001756": 0.75, "320001760": 0.75, "320001752": 0.75,
    "320001775": 0.75, "320001777": 0.75, "320001778": 0.75,
    "320001780": 0.75, "320001781": 0.75, "320001784": 0.75,
    "320001793": 0.75,

    # 건강식품/세트 — 1.00kg
    "320002809": 1.00, "320002766": 1.00, "320002747": 1.00,
    "320002771": 1.00,
}
DEFAULT_WEIGHT = 0.50

CATEGORY_JP_NAME = {
    "320001619": "スキンケア>化粧水",
    "320001621": "スキンケア>クリーム",
    "320001623": "スキンケア>美容液",
    "320001628": "スキンケア>マスク",
    "320001639": "スキンケア>クレンジング",
    "320001663": "メイクアップ>クッション",
    "320001710": "メイクアップ>リップ",
    "320001717": "メイクアップ>チーク",
    "320001741": "スキンケア>日焼け止め",
    "320001756": "ボディケア",
    "320001775": "ヘアケア",
    "320001829": "香水",
}


# ══════════════════════════════════════════════════════════════
# 매칭 함수
# ══════════════════════════════════════════════════════════════

def _match_qoo10_category(item):
    """상품명 키워드 기반 Qoo10 소카테고리 코드 매칭"""
    name = (item.get("name", "") + " " + item.get("name_jp", "")).lower()

    for keywords, code in KEYWORD_CATEGORY_MAP:
        for kw in keywords:
            if kw.lower() in name:
                return code

    cat_id = str(item.get("category_id", "") or item.get("kj_category", ""))
    return DEFAULT_CATEGORY_MAP.get(cat_id, "320001621")


def _match_brand_code(item):
    """브랜드명 → Qoo10 브랜드 코드 매칭 (없으면 공란)"""
    brand = (item.get("brand", "")).lower().strip()
    name = (item.get("name", "")).lower()

    if brand in BRAND_CODE_MAP:
        return BRAND_CODE_MAP[brand]

    for kw, code in BRAND_CODE_MAP.items():
        if kw in name:
            return code

    return ""


def _get_item_weight(category_code):
    """카테고리코드 → 상품 무게(kg)"""
    return CATEGORY_WEIGHT_MAP.get(category_code, DEFAULT_WEIGHT)


# ★ v0.9.1: 홍보성 금지 문구 제거
PROHIBITED_WORDS = [
    '特価', '割引', '破格', 'セール', 'SALE', 'sale',
    '激安', '最安', '限定', '半額', 'OFF', '%OFF',
    '送料無料', '無料配送', 'ポイント', '倍',
    '특가', '할인', '파격', '세일', '한정', '무료배송',
]


def _remove_prohibited_words(name: str) -> str:
    """★ v0.9.1: Qoo10 상품명 홍보성 문구 자동 제거"""
    for word in PROHIBITED_WORDS:
        name = name.replace(word, '')
    name = re.sub(r'\[\s*\]', '', name)
    name = re.sub(r'\(\s*\)', '', name)
    name = re.sub(r'\s{2,}', ' ', name)
    return name.strip()


def _truncate_item_name(name, max_len=50):
    """상품명 50자 제한 + 프로모션 텍스트 제거"""
    if not name:
        return ""
    # 한국어 프로모션 대괄호 제거
    cleaned = re.sub(r'\[.*?(?:특가|한정|세일|할인|이벤트|봄맞이).*?\]\s*', '', name)
    # ★ v0.9.1: 일본어 홍보 금지 문구 제거
    cleaned = _remove_prohibited_words(cleaned)
    if len(cleaned) <= max_len:
        return cleaned
    return cleaned[:max_len]


def _extract_search_keywords(item, category_code):
    """검색어 최대 10개 추출 ($$구분, 각 30글자 제한)"""
    keywords = []
    brand = (item.get("brand", "") or "").strip()
    if brand:
        keywords.append(brand)

    cat_jp = CATEGORY_JP_NAME.get(category_code, "")
    if ">" in cat_jp:
        for part in cat_jp.split(">"):
            p = part.strip()
            if p and p not in keywords:
                keywords.append(p)
    elif cat_jp and cat_jp not in keywords:
        keywords.append(cat_jp)

    for fixed in ["韓国コスメ", "韓国", "Korean Beauty"]:
        if fixed not in keywords:
            keywords.append(fixed)

    name = (item.get("name_jp", "") or item.get("name", "") or "").lower()
    for kw in ["ビタミンC", "コラーゲン", "PDRN", "ヒアルロン酸",
               "シカ", "レチノール", "ナイアシンアミド", "セラミド", "スネイル"]:
        if kw.lower() in name and kw not in keywords:
            keywords.append(kw)

    # ★ v0.9.1: $$ 구분자 + 각 30글자 제한
    return "$$".join([kw[:30] for kw in keywords if kw.strip()][:10])


# ══════════════════════════════════════════════════════════════
# ★ v0.9.1: 공식 50컬럼 헤더 (A~AX)
# H: start_date, L: taxrate 추가
# ══════════════════════════════════════════════════════════════

OFFICIAL_HEADERS = [
    "item_number",                    # A
    "seller_unique_item_id",          # B
    "category_number",                # C
    "brand_number",                   # D
    "item_name",                      # E
    "item_promotion_name",            # F
    "item_status_Y/N/D",              # G
    "start_date",                     # H  ★ v0.9.1 추가
    "end_date",                       # I
    "price_yen",                      # J
    "retail_price_yen",               # K
    "taxrate",                        # L  ★ v0.9.1 추가
    "quantity",                       # M
    "option_info",                    # N
    "additional_option_info",         # O
    "additional_option_text",         # P
    "image_main_url",                 # Q
    "image_other_url",                # R
    "video_url",                      # S
    "image_option_info",              # T
    "image_additional_option_info",   # U
    "header_html",                    # V
    "footer_html",                    # W
    "item_description",               # X
    "Shipping_number",                # Y
    "option_number",                  # Z
    "available_shipping_date",        # AA
    "desired_shipping_date",          # AB
    "search_keyword",                 # AC
    "item_condition_type",            # AD
    "origin_type",                    # AE
    "origin_region_id",               # AF
    "origin_country_id",              # AG
    "origin_others",                  # AH
    "medication_type",                # AI
    "item_weight",                    # AJ
    "item_material",                  # AK
    "model_name",                     # AL
    "external_product_type",          # AM
    "external_product_id",            # AN
    "manufacture_date",               # AO
    "expiration_date_type",           # AP
    "expiration_date_MFD",            # AQ
    "expiration_date_PAO",            # AR
    "expiration_date_EXP",            # AS
    "under18s_display_Y/N",           # AT
    "A/S_info",                       # AU
    "buy_limit_type",                 # AV
    "buy_limit_date",                 # AW
    "buy_limit_qty",                  # AX
]


# ══════════════════════════════════════════════════════════════
# 1. Qoo10 엑셀 생성 ★ v0.9.1
# ══════════════════════════════════════════════════════════════

def generate_qoo10_excel(items: list) -> BytesIO:
    """Qoo10 J·QSM 대량등록용 엑셀 파일 생성 (공식 50컬럼)"""
    try:
        import openpyxl
        from openpyxl.styles import Font, Alignment
    except ImportError:
        logger.error("openpyxl 미설치 – pip install openpyxl")
        return None

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Qoo10_Upload"

    # ── 헤더 (1행) ──
    header_font = Font(bold=True, size=10)
    for col, header in enumerate(OFFICIAL_HEADERS, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    # ── 데이터 행 ──
    cat_stats = {}
    brand_matched = 0
    weight_stats = {}

    for row_idx, item in enumerate(items, 2):
        qoo10_cat = _match_qoo10_category(item)
        cat_stats[qoo10_cat] = cat_stats.get(qoo10_cat, 0) + 1

        qoo10_brand = _match_brand_code(item)
        if qoo10_brand:
            brand_matched += 1

        weight = _get_item_weight(qoo10_cat)
        weight_stats[weight] = weight_stats.get(weight, 0) + 1

        # 이미지
        thumbnail = item.get("thumbnail", "") or item.get("image_url", "")
        detail_images = item.get("detail_images", [])
        # ★ v0.9.1: 구분자 $$ 
        other_images = "$$".join(detail_images[:20]) if detail_images else ""

        # 옵션
        detail = item.get("detail_info", {})
        opt_name = detail.get("option1_name", "")
        opt_values = detail.get("option1_values", [])
        option_str = ""
        if opt_name and opt_values:
            sell_price = item.get("sell_price_jpy", 0)
            option_parts = [f"{v}^{sell_price}^99^0^0" for v in opt_values]
            option_str = f"{opt_name}:#{'$$'.join(option_parts)}"

        # 가격
        sell_price = item.get("sell_price_jpy", 0)
        retail_price = item.get("consumer_price_jpy", 0)
        if retail_price <= sell_price:
            retail_price = math.ceil(sell_price * 1.3)

        # ★ v0.9.1: 상품명 홍보문구 제거 후 50자 제한
        name_jp = item.get("name_jp", item.get("name", ""))
        item_name = _truncate_item_name(name_jp)

        # 검색어
        search_kw = _extract_search_keywords(item, qoo10_cat)

        # ── 50컬럼 행 데이터 (A~AX) ──
        row_data = [
            "",                                                    # A  item_number
            f"KJ{item.get('product_id', '') or item.get('item_id', '')}",  # B  seller_unique_item_id
            qoo10_cat,                                             # C  category_number
            qoo10_brand,                                           # D  brand_number
            item_name,                                             # E  item_name
            "韓国コスメ 正規品",                                     # F  item_promotion_name
            "Y",                                                   # G  item_status
            "",                                                    # H  start_date (공란=즉시)  ★ v0.9.1
            "2030-12-31",                                          # I  end_date               ★ v0.9.1
            sell_price,                                            # J  price_yen
            retail_price,                                          # K  retail_price_yen
            "",                                                    # L  taxrate (공란=기본)     ★ v0.9.1
            100,                                                   # M  quantity
            option_str,                                            # N  option_info
            "",                                                    # O  additional_option_info
            "",                                                    # P  additional_option_text
            thumbnail,                                             # Q  image_main_url
            other_images,                                          # R  image_other_url
            "",                                                    # S  video_url
            "",                                                    # T  image_option_info
            "",                                                    # U  image_additional_option_info
            "",                                                    # V  header_html
            "",                                                    # W  footer_html
            item.get("detail_html", ""),                          # X  item_description
            QOO10_KSE_SHIPPING_CODE,                              # Y  Shipping_number
            "",                                                    # Z  option_number
            "3",                                                   # AA available_shipping_date ★ v0.9.1
            "7",                                                   # AB desired_shipping_date
            search_kw,                                             # AC search_keyword
            "1",                                                   # AD item_condition_type (신품)
            "2",                                                   # AE origin_type (해외)
            "",                                                    # AF origin_region_id
            "KR",                                                  # AG origin_country_id
            "",                                                    # AH origin_others
            "",                                                    # AI medication_type
            str(weight),                                           # AJ item_weight
            "",                                                    # AK item_material
            "",                                                    # AL model_name
            "",                                                    # AM external_product_type
            "",                                                    # AN external_product_id
            "",                                                    # AO manufacture_date
            "",                                                    # AP expiration_date_type
            "",                                                    # AQ expiration_date_MFD
            "",                                                    # AR expiration_date_PAO
            "",                                                    # AS expiration_date_EXP
            "N",                                                   # AT under18s_display
            "",                                                    # AU A/S_info
            "",                                                    # AV buy_limit_type
            "",                                                    # AW buy_limit_date
            "",                                                    # AX buy_limit_qty
        ]

        for col, value in enumerate(row_data, 1):
            ws.cell(row=row_idx, column=col, value=value)

    # ── 열 너비 ──
    col_widths = {
        "A": 12, "B": 18, "C": 15, "D": 12, "E": 50,
        "F": 22, "G": 8, "H": 14, "I": 14, "J": 12,
        "K": 12, "L": 8, "M": 8, "N": 40, "Q": 50,
        "R": 50, "X": 30, "Y": 12, "AC": 40, "AG": 8, "AJ": 8,
    }
    for col_letter, width in col_widths.items():
        ws.column_dimensions[col_letter].width = width

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    logger.info(f"[엑셀] Qoo10 업로드 파일 생성: {len(items)}건 (50컬럼)")
    logger.info(f"[엑셀] 브랜드 매칭: {brand_matched}/{len(items)}건")
    top5_cat = sorted(cat_stats.items(), key=lambda x: -x[1])[:5]
    logger.info(f"[엑셀] 카테고리 TOP5: {dict(top5_cat)}")
    logger.info(f"[엑셀] 무게 분포: {dict(sorted(weight_stats.items()))}")

    return output


# ══════════════════════════════════════════════════════════════
# 2. Google Drive 업로드 (변경 없음)
# ══════════════════════════════════════════════════════════════

def upload_to_drive(file_bytes: BytesIO, filename: str = None) -> str:
    """엑셀 파일을 Google Drive에 업로드, 실패 시 로컬 저장"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"qoo10_upload_{timestamp}.xlsx"

    artifact_dir = os.path.join(os.getcwd(), "artifacts")
    os.makedirs(artifact_dir, exist_ok=True)
    local_path = os.path.join(artifact_dir, filename)

    file_bytes.seek(0)
    with open(local_path, "wb") as f:
        f.write(file_bytes.read())
    logger.info(f"[로컬] artifacts 백업 완료: {local_path}")

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
            sa_info, scopes=["https://www.googleapis.com/auth/drive"]
        )
        service = build("drive", "v3", credentials=credentials)

        file_metadata = {
            "name": filename,
            "parents": [QOO10_DRIVE_FOLDER_ID],
            "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        }
        file_bytes.seek(0)
        media = MediaIoBaseUpload(
            file_bytes,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            resumable=True,
        )
        file = service.files().create(
            body=file_metadata, media_body=media,
            fields="id, webViewLink", supportsAllDrives=True,
        ).execute()

        web_link = file.get("webViewLink", "")
        file_id = file.get("id", "")
        logger.info(f"[Drive] 업로드 성공: {web_link}")

        try:
            service.permissions().create(
                fileId=file_id,
                body={"type": "user", "role": "owner",
                      "emailAddress": "soccercamp.beta@gmail.com"},
                transferOwnership=True, supportsAllDrives=True,
            ).execute()
            logger.info("[Drive] 소유권 이전 완료")
        except Exception as perm_err:
            logger.warning(f"[Drive] 소유권 이전 실패 (무시): {perm_err}")

        return web_link

    except ImportError:
        logger.warning("google-api-python-client 미설치 → 로컬 백업만 사용")
        return f"LOCAL:{local_path}"
    except Exception as e:
        logger.warning(f"[Drive] 업로드 실패 (로컬 백업 사용): {e}")
        return f"LOCAL:{local_path}"


# ══════════════════════════════════════════════════════════════
# 3. 통합 실행 (변경 없음)
# ══════════════════════════════════════════════════════════════

def generate_and_upload(items: list) -> dict:
    """최종 통과 상품 → 엑셀 생성 → Drive 업로드 (실패 시 로컬 저장)"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"qoo10_upload_{timestamp}.xlsx"

    excel_bytes = generate_qoo10_excel(items)
    if excel_bytes is None:
        return {"item_count": 0, "filename": "", "drive_url": "", "success": False}

    drive_url = upload_to_drive(excel_bytes, filename)
    is_local = drive_url.startswith("LOCAL:") if drive_url else False

    result = {
        "item_count": len(items),
        "filename": filename,
        "drive_url": drive_url,
        "success": bool(drive_url),
        "is_local": is_local,
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
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    test_items = [
        {
            "name": "어성초 77% 수딩 토너",
            "name_jp": "【ANUA】ドクダミ77% スージングトナー 250ml",
            "brand": "ANUA",
            "item_id": "12345",
            "product_id": "12345",
            "supply_price": 8000,
            "sell_price_jpy": 2600,
            "margin_rate": 0.217,
            "final_score": 82,
            "grade": "A",
            "thumbnail": "https://example.com/thumb1.jpg",
            "detail_images": [
                "https://example.com/detail1.jpg",
                "https://example.com/detail2.jpg",
            ],
            "detail_html": "<div>テスト商品説明</div>",
            "detail_info": {
                "option1_name": "容量",
                "option1_values": ["250ml", "500ml"],
            },
        },
    ]

    excel = generate_qoo10_excel(test_items)
    if excel:
        with open("test_qoo10_upload.xlsx", "wb") as f:
            f.write(excel.read())
        print("테스트 엑셀 생성 완료: test_qoo10_upload.xlsx")
        print(f"헤더 수: {len(OFFICIAL_HEADERS)}컬럼 (A~AX)")
    else:
        print("엑셀 생성 실패")
