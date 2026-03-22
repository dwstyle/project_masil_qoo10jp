"""
translator.py – 일본어 번역 & Qoo10 상세페이지 HTML 생성 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.8

변경사항 (v0.7 → v0.8):
  - yakujiho_filter 연동: 번역 전 한국어 필터 + 번역 후 일본어 필터
  - build_detail_html에서 header/footer 분리 (product_analyzer.py로 이관)
  - detail_html은 X열 전용 — 상세 이미지 삽입만 담당
  - translate_to_japanese, translate_batch에 약기법 2중 필터 적용

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
import html as html_module

logger = logging.getLogger(__name__)

# ★ v0.8: 약기법 필터 import
try:
    from yakujiho_filter import sanitize_jp, sanitize_kr, sanitize_html
    YAKUJIHO_ENABLED = True
    logger.info("[약기법] yakujiho_filter 로드 완료")
except ImportError:
    logger.warning("[약기법] yakujiho_filter 모듈 없음 – 필터링 비활성화")
    YAKUJIHO_ENABLED = False
    def sanitize_jp(text): return text, [], 0
    def sanitize_kr(text): return text
    def sanitize_html(text): return text


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
    한국어 텍스트를 일본어로 번역 + 약기법 필터 적용
    Args:
        text: 원본 텍스트 (한국어)
        client: Translation 클라이언트 (없으면 자동 생성)
    Returns:
        일본어 번역 텍스트 (실패 시 원본 반환)
    """
    if not text or not text.strip():
        return text

    # ★ v0.8: 번역 전 한국어 약기법 필터
    text = sanitize_kr(text)

    if client is None:
        client = _get_translate_client()

    if client is None:
        logger.warning("[번역] 클라이언트 없음 – 원본 반환")
        return text

    try:
        result = client.translate(text, source_language="ko", target_language="ja")
        translated = result.get("translatedText", text)
        translated = _decode_html_entities(translated)

        # ★ v0.8: 번역 후 일본어 약기법 필터
        translated, footnotes, count = sanitize_jp(translated)
        if count > 0:
            logger.info(f"[약기법] 번역 결과에서 {count}건 키워드 치환")

        logger.debug(f"[번역] '{text[:30]}' → '{translated[:30]}'")
        return translated
    except Exception as e:
        logger.error(f"[번역] 오류: {e}")
        return text


def translate_batch(texts: list, client=None) -> list:
    """
    여러 텍스트를 일괄 번역 + 약기법 필터 (API 호출 최소화)
    Args:
        texts: 한국어 텍스트 리스트
        client: Translation 클라이언트
    Returns:
        일본어 텍스트 리스트
    """
    if not texts:
        return []

    # ★ v0.8: 번역 전 한국어 약기법 필터
    texts = [sanitize_kr(t) for t in texts]

    if client is None:
        client = _get_translate_client()

    if client is None:
        return texts

    try:
        results = client.translate(texts, source_language="ko", target_language="ja")
        translated = []
        filter_total = 0
        for r in results:
            t = r.get("translatedText", "")
            t = _decode_html_entities(t)
            # ★ v0.8: 번역 후 일본어 약기법 필터
            t, _, count = sanitize_jp(t)
            filter_total += count
            translated.append(t)

        logger.info(f"[번역] {len(texts)}건 일괄 번역 완료 (약기법 {filter_total}건 치환)")
        return translated
    except Exception as e:
        logger.error(f"[번역 일괄] 오류: {e}")
        return texts


def _decode_html_entities(text: str) -> str:
    """HTML 엔티티 디코딩"""
    return html_module.unescape(text)


# ══════════════════════════════════════════════════════════════
# 2. 상품명 번역 + 최적화
# ══════════════════════════════════════════════════════════════

def translate_product_name(name_kr: str, brand: str = "", client=None) -> str:
    """
    상품명을 큐텐 재팬용으로 번역·최적화
    - 브랜드명은 영문 그대로 유지
    - 한국어 상품 설명만 번역
    - 약기법 금지 키워드 자동 치환
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

    # 한국어 부분만 번역 (약기법 필터 포함)
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
# 3. Qoo10 상세페이지 HTML 생성 ★ v0.8: 경량화
# ══════════════════════════════════════════════════════════════

def build_detail_html(product: dict, detail_images: list, client=None) -> str:
    """
    ★ v0.8: X열(item_description) 전용
    header_html(V열)과 footer_html(W열)은 product_analyzer.py가 생성.
    이 함수는 상세 이미지 삽입만 담당.

    Args:
        product: 상품 정보 dict
        detail_images: 상세 이미지 URL 리스트
        client: Translation 클라이언트
    Returns:
        HTML 문자열
    """
    # 상품명 (이미 번역된 게 있으면 사용)
    name_jp = product.get("name_jp", "")
    if not name_jp:
        name_kr = product.get("name", "")
        brand = product.get("brand", "")
        name_jp = translate_product_name(name_kr, brand, client)

    # 이미지 태그 생성
    img_tags = ""
    for url in detail_images:
        img_tags += (
            f'  <img src="{url}" width="750" '
            f'style="max-width:100%; height:auto; display:block; margin:0 auto 10px auto;" '
            f'alt="{name_jp}">\n'
        )

    html_content = f"""<div style="text-align:center; max-width:750px; margin:0 auto;">
{img_tags}
</div>"""

    # ★ v0.8: 약기법 필터 적용 (이미지 alt 텍스트 등)
    html_content = sanitize_html(html_content)

    return html_content


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

    # 일괄 번역 (50개씩 청크) — 약기법 필터 포함
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

    logger.info(f"[번역] {len(valid_names)}건 상품명 번역 완료 (약기법 필터 적용)")
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

        item_html = build_detail_html(item, detail_images, client)
        item["detail_html"] = item_html

    generated = sum(1 for item in items if item.get("detail_html"))
    logger.info(f"[HTML] {generated}/{len(items)}건 상세페이지 생성 완료 (약기법 필터 적용)")
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

    # 약기법 필터 테스트 (번역 없이)
    print("=== 약기법 필터 테스트 ===")
    test_kr_words = [
        "안티에이징 주름개선 미백 크림",
        "화이트닝 톤업 세럼",
        "여드름치료 디톡스 마스크",
    ]
    for word in test_kr_words:
        filtered = sanitize_kr(word)
        print(f"  KR: '{word}' → '{filtered}'")

    print()

    # HTML 생성 테스트 (번역 없이 구조 확인)
    test_product = {
        "name": "어성초 77% 수딩 토너 250ml",
        "name_jp": "【ANUA】ドクダミ77% スージングトナー 250ml",
        "brand": "ANUA",
        "category": "토너/스킨",
    }
    test_images = [
        "https://kmclubb2b.com/home/data/editor/2026/01/27/kbm-d5-1.jpg",
        "https://kmclubb2b.com/home/data/editor/2026/01/27/kbm-d5-2.jpg",
        "https://kmclubb2b.com/home/data/editor/2026/01/27/kbm-d5-3.jpg",
    ]

    detail_html = build_detail_html(test_product, test_images)
    print("=== Detail HTML 생성 완료 ===")
    print(f"HTML 길이: {len(detail_html)}자")
    print(f"약기법 필터 활성화: {YAKUJIHO_ENABLED}")
    print("\n--- HTML 미리보기 (앞 500자) ---")
    print(detail_html[:500])