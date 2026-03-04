"""
translator.py – 일본어 번역 & Qoo10 상세페이지 HTML 생성 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.7

번역 전략:
  - Google Cloud Translation API (Basic) 사용
  - 월 500,000자 무료 티어
  - 상품명 + 핵심 필드만 번역 (비용 최소화)
  - 상세페이지는 일본어 HTML 텍스트 + KJ9603 원본 이미지
"""

import os
import json
import logging
import re

logger = logging.getLogger(__name__)

# GCP 서비스 계정은 GOOGLE_SERVICE_ACCOUNT_JSON 환경변수로 인증
# google-cloud-translate 패키지 필요


# ══════════════════════════════════════════════════════════════
# 1. 번역 엔진
# ══════════════════════════════════════════════════════════════

def _get_translate_client():
    """Google Cloud Translation 클라이언트 생성"""
    try:
        from google.cloud import translate_v2 as translate
        from google.oauth2 import service_account

        sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        if sa_json:
            sa_info = json.loads(sa_json)
            credentials = service_account.Credentials.from_service_account_info(sa_info)
            client = translate.Client(credentials=credentials)
        else:
            client = translate.Client()

        return client
    except ImportError:
        logger.error("google-cloud-translate 미설치 – pip install google-cloud-translate")
        return None
    except Exception as e:
        logger.error(f"Translation 클라이언트 생성 실패: {e}")
        return None


def translate_to_japanese(text: str, client=None) -> str:
    """
    한국어 텍스트를 일본어로 번역
    Args:
        text: 원본 텍스트 (한국어)
        client: Translation 클라이언트 (없으면 자동 생성)
    Returns:
        일본어 번역 텍스트 (실패 시 원본 반환)
    """
    if not text or not text.strip():
        return text

    if client is None:
        client = _get_translate_client()

    if client is None:
        logger.warning("[번역] 클라이언트 없음 – 원본 반환")
        return text

    try:
        result = client.translate(text, source_language="ko", target_language="ja")
        translated = result.get("translatedText", text)
        # HTML 엔티티 디코딩
        translated = _decode_html_entities(translated)
        logger.debug(f"[번역] '{text[:30]}' → '{translated[:30]}'")
        return translated
    except Exception as e:
        logger.error(f"[번역] 오류: {e}")
        return text


def translate_batch(texts: list, client=None) -> list:
    """
    여러 텍스트를 일괄 번역 (API 호출 최소화)
    Args:
        texts: 한국어 텍스트 리스트
        client: Translation 클라이언트
    Returns:
        일본어 텍스트 리스트
    """
    if not texts:
        return []

    if client is None:
        client = _get_translate_client()

    if client is None:
        return texts

    try:
        results = client.translate(texts, source_language="ko", target_language="ja")
        translated = []
        for r in results:
            t = r.get("translatedText", "")
            translated.append(_decode_html_entities(t))
        logger.info(f"[번역] {len(texts)}건 일괄 번역 완료")
        return translated
    except Exception as e:
        logger.error(f"[번역 일괄] 오류: {e}")
        return texts


def _decode_html_entities(text: str) -> str:
    """HTML 엔티티 디코딩"""
    import html
    return html.unescape(text)


# ══════════════════════════════════════════════════════════════
# 2. 상품명 번역 + 최적화
# ══════════════════════════════════════════════════════════════

def translate_product_name(name_kr: str, brand: str = "", client=None) -> str:
    """
    상품명을 큐텐 재팬용으로 번역·최적화
    - 브랜드명은 영문 그대로 유지
    - 한국어 상품 설명만 번역
    - SEO 키워드 추가 (韓国コスメ 등)
    """
    if not name_kr:
        return ""

    # 브랜드명 분리 (영문은 번역하지 않음)
    brand_part = ""
    name_part = name_kr

    if brand:
        brand_part = brand
        name_part = name_kr.replace(brand, "").strip()

    # 영문/숫자 부분 보존
    preserved = []
    pattern = r'[A-Za-z0-9]+(?:\s*[A-Za-z0-9]+)*'
    for match in re.finditer(pattern, name_part):
        preserved.append(match.group())

    # 한국어 부분만 번역
    translated = translate_to_japanese(name_part, client)

    # 브랜드 + 번역 조합
    if brand_part:
        final_name = f"【{brand_part}】{translated}"
    else:
        final_name = translated

    # 길이 제한 (큐텐 상품명 최대 100자 권장)
    if len(final_name) > 100:
        final_name = final_name[:97] + "..."

    return final_name


# ══════════════════════════════════════════════════════════════
# 3. Qoo10 상세페이지 HTML 생성
# ══════════════════════════════════════════════════════════════

def build_detail_html(product: dict, detail_images: list, client=None) -> str:
    """
    Qoo10 상세페이지용 HTML 생성
    - 일본어 텍스트 + KJ9603 원본 이미지
    - 정품보증·배송안내·주의사항 포함

    Args:
        product: 상품 정보 dict
        detail_images: 상세 이미지 URL 리스트
        client: Translation 클라이언트
    Returns:
        HTML 문자열
    """
    # 상품명 번역
    name_kr = product.get("name", "")
    brand = product.get("brand", "")
    name_jp = translate_product_name(name_kr, brand, client)

    # 카테고리 번역
    category = product.get("category", "")
    category_jp = _category_kr_to_jp(category)

    # 이미지 태그 생성
    img_tags = ""
    for url in detail_images:
        img_tags += f'      <img src="{url}" width="750" style="max-width:100%; height:auto; display:block; margin:0 auto 10px auto;" alt="{name_jp}">\n'

    # HTML 조립
    html = f"""<div style="text-align:center; font-family:'Hiragino Sans','Meiryo',sans-serif; max-width:750px; margin:0 auto; padding:10px; color:#333;">

  <!-- 헤더 -->
  <div style="background:#f8f8f8; padding:20px; border-radius:8px; margin-bottom:20px;">
    <h2 style="font-size:20px; margin:0 0 10px 0; color:#222;">{name_jp}</h2>
    <p style="font-size:14px; color:#888; margin:5px 0;">🇰🇷 韓国コスメ ｜ {category_jp}</p>
  </div>

  <!-- 정품보증 배너 -->
  <div style="background:#fff3cd; padding:12px; border-radius:6px; margin-bottom:20px; border:1px solid #ffc107;">
    <p style="font-size:14px; margin:0; color:#856404;">
      ✅ <strong>正規品100%保証</strong> ｜ 韓国メーカー正規取引先から直接仕入れ
    </p>
  </div>

  <!-- 商品情報 -->
  <div style="text-align:left; padding:15px; background:#fafafa; border-radius:6px; margin-bottom:20px;">
    <table style="width:100%; font-size:13px; border-collapse:collapse;">
      <tr>
        <td style="padding:8px; border-bottom:1px solid #eee; width:30%; color:#666;">ブランド</td>
        <td style="padding:8px; border-bottom:1px solid #eee;">{brand or name_kr[:20]}</td>
      </tr>
      <tr>
        <td style="padding:8px; border-bottom:1px solid #eee; color:#666;">原産国</td>
        <td style="padding:8px; border-bottom:1px solid #eee;">韓国 🇰🇷</td>
      </tr>
      <tr>
        <td style="padding:8px; border-bottom:1px solid #eee; color:#666;">カテゴリー</td>
        <td style="padding:8px; border-bottom:1px solid #eee;">{category_jp}</td>
      </tr>
      <tr>
        <td style="padding:8px; color:#666;">商品タイプ</td>
        <td style="padding:8px;">韓国コスメ</td>
      </tr>
    </table>
  </div>

  <!-- 구분선 -->
  <hr style="border:none; border-top:2px solid #e0e0e0; margin:25px 0;">

  <!-- 상세 이미지 -->
  <div style="margin-bottom:25px;">
{img_tags}
  </div>

  <!-- 구분선 -->
  <hr style="border:none; border-top:2px solid #e0e0e0; margin:25px 0;">

  <!-- 배송 안내 -->
  <div style="text-align:left; padding:15px; background:#e8f5e9; border-radius:6px; margin-bottom:15px;">
    <p style="font-size:15px; font-weight:bold; margin:0 0 8px 0; color:#2e7d32;">📦 配送について</p>
    <ul style="font-size:13px; color:#555; margin:0; padding-left:20px; line-height:1.8;">
      <li>韓国から直接発送いたします</li>
      <li>発送後 3〜7営業日でお届け（税関状況により前後する場合があります）</li>
      <li>送料無料</li>
      <li>追跡番号は発送後にご案内いたします</li>
    </ul>
  </div>

  <!-- 주의사항 -->
  <div style="text-align:left; padding:15px; background:#fff8e1; border-radius:6px; margin-bottom:15px;">
    <p style="font-size:15px; font-weight:bold; margin:0 0 8px 0; color:#f57f17;">⚠️ ご注意事項</p>
    <ul style="font-size:12px; color:#666; margin:0; padding-left:20px; line-height:1.8;">
      <li>海外発送商品のため、お届けまでに通常より日数がかかる場合があります</li>
      <li>パッケージデザインは予告なく変更される場合があります</li>
      <li>商品説明の一部に韓国語表記が含まれる場合があります</li>
      <li>お客様都合による返品・交換はお受けできない場合があります</li>
      <li>個人輸入品として税関で課税される場合、お客様のご負担となります</li>
    </ul>
  </div>

  <!-- 판매자 정보 -->
  <div style="text-align:center; padding:15px; background:#f5f5f5; border-radius:6px; margin-top:20px;">
    <p style="font-size:12px; color:#999; margin:0;">
      Plan B Cabinet ｜ 韓国正規取引先からの直送品
    </p>
  </div>

</div>"""

    return html


# ══════════════════════════════════════════════════════════════
# 4. 일괄 처리
# ══════════════════════════════════════════════════════════════

def translate_items_batch(items: list) -> list:
    """
    상품 리스트의 이름을 일괄 번역
    Args:
        items: 상품 리스트 (name 필드 필요)
    Returns:
        items에 name_jp 필드가 추가된 리스트
    """
    client = _get_translate_client()

    if client is None:
        logger.warning("[번역] 클라이언트 없음 – 원본 이름 사용")
        for item in items:
            item["name_jp"] = item.get("name", "")
        return items

    # 번역할 이름 수집
    names_kr = [item.get("name", "") for item in items]

    # 빈 문자열 필터링
    valid_indices = [i for i, n in enumerate(names_kr) if n.strip()]
    valid_names = [names_kr[i] for i in valid_indices]

    if not valid_names:
        return items

    # 일괄 번역 (50개씩 청크)
    chunk_size = 50
    all_translated = []
    for i in range(0, len(valid_names), chunk_size):
        chunk = valid_names[i:i + chunk_size]
        translated_chunk = translate_batch(chunk, client)
        all_translated.extend(translated_chunk)

    # 결과 매핑
    t_idx = 0
    for i in valid_indices:
        if t_idx < len(all_translated):
            items[i]["name_jp"] = all_translated[t_idx]
            t_idx += 1

    # 번역 안 된 항목은 원본 사용
    for item in items:
        if "name_jp" not in item:
            item["name_jp"] = item.get("name", "")

    logger.info(f"[번역] {len(valid_names)}건 상품명 번역 완료")
    return items


def generate_detail_html_batch(items: list) -> list:
    """
    상품 리스트에 대해 일괄 상세페이지 HTML 생성
    Args:
        items: 상품 리스트 (detail_images 필드 필요)
    Returns:
        items에 detail_html 필드가 추가된 리스트
    """
    client = _get_translate_client()

    for item in items:
        detail_images = item.get("detail_images", [])
        if not detail_images:
            item["detail_html"] = ""
            continue

        html = build_detail_html(item, detail_images, client)
        item["detail_html"] = html

    generated = sum(1 for item in items if item.get("detail_html"))
    logger.info(f"[HTML] {generated}/{len(items)}건 상세페이지 생성 완료")
    return items


# ══════════════════════════════════════════════════════════════
# 5. 카테고리 한→일 매핑
# ══════════════════════════════════════════════════════════════

def _category_kr_to_jp(category_kr: str) -> str:
    """KJ9603 카테고리를 일본어로 변환"""
    mapping = {
        "스킨케어": "スキンケア",
        "토너/스킨": "化粧水・トナー",
        "세럼/에센스": "美容液・セラム",
        "크림": "クリーム",
        "로션/에멀전": "乳液・ローション",
        "클렌징": "クレンジング",
        "마스크팩": "パック・マスク",
        "선케어": "日焼け止め・UVケア",
        "립메이크업": "リップメイク",
        "아이메이크업": "アイメイク",
        "페이스메이크업": "フェイスメイク",
        "베이스메이크업": "ベースメイク",
        "헤어케어": "ヘアケア",
        "헤어샴푸": "シャンプー",
        "헤어트리트먼트": "トリートメント",
        "바디케어": "ボディケア",
        "바디워시": "ボディウォッシュ",
        "바디로션": "ボディローション",
        "향수": "香水・フレグランス",
        "네일": "ネイル",
        "뷰티툴": "メイクツール",
        "세트/키트": "セット・キット",
    }
    return mapping.get(category_kr, "韓国コスメ")


# ══════════════════════════════════════════════════════════════
# 직접 실행 (테스트)
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # HTML 생성 테스트 (번역 없이 구조 확인)
    test_product = {
        "name": "어성초 77% 수딩 토너 250ml",
        "brand": "ANUA",
        "category": "토너/스킨",
    }
    test_images = [
        "https://kmclubb2b.com/home/data/editor/2026/01/27/kbm-d5-1.jpg",
        "https://kmclubb2b.com/home/data/editor/2026/01/27/kbm-d5-2.jpg",
        "https://kmclubb2b.com/home/data/editor/2026/01/27/kbm-d5-3.jpg",
    ]

    html = build_detail_html(test_product, test_images)
    print("HTML 생성 완료!")
    print(f"HTML 길이: {len(html)}자")
    print("\n--- HTML 미리보기 (앞 500자) ---")
    print(html[:500])
