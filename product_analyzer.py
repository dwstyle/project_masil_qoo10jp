"""
product_analyzer.py – Gemini Vision 이미지 분석 + 올리브영 검색 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 1.0

기능:
  - Gemini 2.5 Flash API로 상세 이미지 분석 (성분, 효과, 마케팅 포인트 추출)
  - Google Search Grounding으로 올리브영 정보 검색
  - 일본어 header_html / footer_html 자동 생성
  - 약기법 필터 적용
  
비용: 무료 (Gemini Free Tier – 500 RPD)
"""

import os
import json
import logging
import time
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ── 설정 ──
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-2.5-flash"
MAX_IMAGES_PER_PRODUCT = 5  # 비용 절약: 상세 이미지 최대 5장
API_DELAY = 4.5  # 분당 10회 제한 → 6초 간격 (여유 포함)


# ══════════════════════════════════════════════════════════════
# 1. Gemini API 클라이언트
# ══════════════════════════════════════════════════════════════

def _get_gemini_client():
    """Gemini API 클라이언트 생성"""
    try:
        from google import genai
        api_key = GEMINI_API_KEY or os.environ.get("GEMINI_API_KEY", "")
        if not api_key:
            logger.error("[Gemini] API 키 없음 – GEMINI_API_KEY 환경변수 설정 필요")
            return None
        client = genai.Client(api_key=api_key)
        return client
    except ImportError:
        logger.error("google-genai 미설치 – pip install google-genai")
        return None
    except Exception as e:
        logger.error(f"[Gemini] 클라이언트 생성 실패: {e}")
        return None


def _download_image_bytes(url: str) -> Optional[bytes]:
    """이미지 URL에서 바이트 다운로드"""
    try:
        import requests
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200 and len(resp.content) > 1000:
            return resp.content
        return None
    except Exception as e:
        logger.warning(f"[이미지] 다운로드 실패: {url} – {e}")
        return None


# ══════════════════════════════════════════════════════════════
# 2. 이미지 분석 (Gemini Vision)
# ══════════════════════════════════════════════════════════════

VISION_PROMPT = """당신은 한국 화장품/뷰티 상품 상세페이지 분석 전문가입니다.
아래 이미지들은 하나의 상품 상세페이지입니다.

## 1단계: 정보 추출 (한국어)
이미지에서 다음 정보를 최대한 추출하세요:
- product_name: 상품명
- brand: 브랜드명
- volume: 용량/사이즈 (예: 50ml, 200g)
- ingredients: 주요 성분 (최대 5개, 배열)
- effects: 효과/효능 (최대 5개, 배열)
- usage: 사용법 (1~2문장)
- marketing_points: 마케팅 포인트 (수상, 랭킹, 인증, 특허, 판매량, 만족도 등, 배열)
- selling_phrases: 이미지에서 강조된 핵심 셀링 문구 — 큰 글씨, 컬러 강조, 중앙 배치된 텍스트 (배열)
- certifications: 인증 마크 (비건, 더마, EWG, KFDA 등, 배열)
- reviews_summary: 리뷰/후기 관련 내용 요약 (만족도, 재구매율, 소비자 반응 등)
- texture_feel: 사용감 관련 정보 (텍스처, 향, 흡수력, 발림성 등)
- before_after: 사용 전후 효과 설명이 있으면 요약

## 2단계: 일본어 마케팅 요약 생성
위 정보를 바탕으로 Qoo10 일본 소비자를 위한 마케팅 요약을 일본어로 작성하세요.
- 자연스러운 일본어 (번역체 금지)
- 톤: 신뢰감 + 트렌디
- headline: 구매 욕구를 자극하는 1줄 캐치카피 (15~25자)
- marketing_message: 상품 헤더 위에 표시할 강력한 마케팅 한 줄 (리뷰 기반, 수상, 랭킹, 판매량 등 근거 있는 문구)
- points: 3~5개 핵심 포인트 (구체적인 수치/성분명 포함)
- review_highlight: 실제 리뷰/후기 기반 한 줄 요약 (있으면)
- texture_jp: 사용감 한 줄 요약 (있으면)
- recommended_for: 추천 대상 2~4개
- usage_jp: 사용법 일본어

- 절대 사용 금지 키워드: アンチエイジング, ホワイトニング, 美白, シワ改善, 治療, 再生, 若返り, 除菌, 殺菌
  → 대체: エイジングケア, トーンアップ, 透明感, ハリを与える, ケア, すこやかな肌へ, いきいきとした印象, 清潔に保つ

## 출력: 반드시 JSON만 출력 (마크다운 금지)
{
  "extracted": {
    "product_name": "",
    "brand": "",
    "volume": "",
    "ingredients": [],
    "effects": [],
    "usage": "",
    "marketing_points": [],
    "selling_phrases": [],
    "certifications": [],
    "reviews_summary": "",
    "texture_feel": "",
    "before_after": ""
  },
  "summary_jp": {
    "headline": "일본어 1줄 캐치카피",
    "marketing_message": "구매전환을 높이는 근거 있는 마케팅 한 줄",
    "points": ["포인트1", "포인트2", "포인트3"],
    "review_highlight": "리뷰 기반 한 줄 (없으면 빈 문자열)",
    "texture_jp": "사용감 한 줄 (없으면 빈 문자열)",
    "recommended_for": ["こんな方におすすめ1", "こんな方におすすめ2"],
    "usage_jp": "일본어 사용법"
  }
}"""

def analyze_product_images(detail_images: list, client=None) -> dict:
    """
    상세 이미지를 Gemini Vision으로 분석하여 마케팅 정보 추출.

    Args:
        detail_images: 상세 이미지 URL 리스트
        client: Gemini 클라이언트 (없으면 자동 생성)
    Returns:
        분석 결과 dict (extracted + summary_jp)
    """
    if client is None:
        client = _get_gemini_client()
    if client is None:
        return _empty_analysis()

    # 이미지 다운로드 (최대 N장)
    images_to_send = []
    for url in detail_images[:MAX_IMAGES_PER_PRODUCT]:
        img_bytes = _download_image_bytes(url)
        if img_bytes:
            images_to_send.append(img_bytes)

    if not images_to_send:
        logger.warning("[분석] 유효한 이미지 없음")
        return _empty_analysis()

    try:
        from google.genai import types

        # 멀티모달 콘텐츠 구성
        parts = []
        for img_bytes in images_to_send:
            parts.append(types.Part.from_bytes(data=img_bytes, mime_type="image/jpeg"))
        parts.append(types.Part.from_text(text=VISION_PROMPT))

        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[types.Content(parts=parts)],
            config=types.GenerateContentConfig(
                temperature=0.3,
                max_output_tokens=2000,
            ),
        )

        # JSON 파싱
        raw_text = response.text.strip()
        # 마크다운 코드블록 제거
        raw_text = re.sub(r'^```json\s*', '', raw_text)
        raw_text = re.sub(r'\s*```$', '', raw_text)

        result = json.loads(raw_text)
        logger.info(f"[분석] 성공: {result.get('extracted', {}).get('product_name', 'unknown')}")
        return result

    except json.JSONDecodeError as e:
        logger.error(f"[분석] JSON 파싱 실패: {e}")
        logger.debug(f"[분석] 원본 응답: {raw_text[:500]}")
        return _empty_analysis()
    except Exception as e:
        logger.error(f"[분석] Gemini API 오류: {e}")
        return _empty_analysis()


def _empty_analysis() -> dict:
    """분석 실패 시 빈 결과"""
    return {
        "extracted": {
            "product_name": "", "brand": "", "volume": "",
            "ingredients": [], "effects": [], "usage": "",
            "marketing_points": [], "selling_phrases": [],
            "certifications": [], "reviews_summary": "",
        },
        "summary_jp": {
            "headline": "", "points": [],
            "recommended_for": [], "usage_jp": "",
        },
    }


# ══════════════════════════════════════════════════════════════
# 3. 올리브영 검색 (Gemini + Google Search Grounding)
# ══════════════════════════════════════════════════════════════

OLIVEYOUNG_PROMPT = """다음 한국 화장품의 올리브영(oliveyoung.co.kr) 정보를 검색해서 알려주세요.

상품: {brand} {product_name}

찾아야 할 정보:
1. 올리브영 랭킹 (카테고리명, 순위)
2. 캐치프레이즈 또는 마케팅 문구
3. 평점과 리뷰 수
4. 수상 이력 (올리브영 어워드 등)

정보를 못 찾으면 각 필드에 빈 문자열 또는 0을 넣어주세요.

반드시 JSON만 출력:
{{
  "found": true/false,
  "ranking": "카테고리명 N위 또는 빈 문자열",
  "catchphrase": "캐치프레이즈 또는 빈 문자열",
  "rating": 0.0,
  "review_count": 0,
  "awards": []
}}"""


def search_oliveyoung(brand: str, product_name: str, client=None) -> dict:
    """
    Gemini + Google Search Grounding으로 올리브영 정보 검색.

    Args:
        brand: 브랜드명
        product_name: 상품명
        client: Gemini 클라이언트
    Returns:
        올리브영 정보 dict
    """
    if client is None:
        client = _get_gemini_client()
    if client is None:
        return _empty_oliveyoung()

    try:
        from google.genai import types

        prompt = OLIVEYOUNG_PROMPT.format(brand=brand, product_name=product_name)

        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                temperature=0.1,
                max_output_tokens=500,
                tools=[types.Tool(google_search=types.GoogleSearch())],
            ),
        )

        raw_text = response.text.strip()
        raw_text = re.sub(r'^```json\s*', '', raw_text)
        raw_text = re.sub(r'\s*```$', '', raw_text)

        result = json.loads(raw_text)
        if result.get("found"):
            logger.info(f"[올리브영] 매칭 성공: {brand} {product_name}")
        else:
            logger.info(f"[올리브영] 매칭 실패: {brand} {product_name}")
        return result

    except Exception as e:
        logger.error(f"[올리브영] 검색 오류: {e}")
        return _empty_oliveyoung()


def _empty_oliveyoung() -> dict:
    return {
        "found": False, "ranking": "", "catchphrase": "",
        "rating": 0.0, "review_count": 0, "awards": [],
    }


# ══════════════════════════════════════════════════════════════
# 4. Header / Footer HTML 생성
# ══════════════════════════════════════════════════════════════

FOOTER_HTML = """<div style="text-align:center; font-family:'Hiragino Sans','Meiryo',sans-serif; max-width:750px; margin:0 auto; padding:10px; color:#333;">

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

  <!-- 교환/반품 -->
  <div style="text-align:left; padding:15px; background:#fafafa; border-radius:6px; margin-bottom:15px;">
    <p style="font-size:15px; font-weight:bold; margin:0 0 8px 0; color:#555;">🔄 交換・返品について</p>
    <ul style="font-size:13px; color:#555; margin:0; padding-left:20px; line-height:1.8;">
      <li>商品到着後7日以内にご連絡ください</li>
      <li>未開封・未使用の場合のみ交換・返品可能です</li>
      <li>不良品の場合は全額こちらで負担いたします</li>
      <li>お客様都合の返品送料はお客様ご負担となります</li>
    </ul>
  </div>

  <!-- FAQ -->
  <div style="text-align:left; padding:15px; background:#fff8e1; border-radius:6px; margin-bottom:15px;">
    <p style="font-size:15px; font-weight:bold; margin:0 0 8px 0; color:#f57f17;">❓ よくある質問</p>
    <div style="font-size:13px; color:#555; line-height:1.8;">
      <p style="margin:8px 0 3px 0;"><strong>Q. 正規品ですか？</strong></p>
      <p style="margin:0 0 8px 0;">A. はい、全商品100%正規品です。万が一偽物の場合は全額返金いたします。</p>
      <p style="margin:8px 0 3px 0;"><strong>Q. 届くまでどのくらいかかりますか？</strong></p>
      <p style="margin:0 0 8px 0;">A. 注文確定後、3～7営業日以内にお届けします。</p>
      <p style="margin:8px 0 3px 0;"><strong>Q. 届いた商品に問題がある場合は？</strong></p>
      <p style="margin:0;">A. Qoo10メッセージでご連絡ください。迅速に対応いたします。</p>
    </div>
  </div>

  <!-- 주의사항 -->
  <div style="text-align:left; padding:15px; background:#fce4ec; border-radius:6px; margin-bottom:15px;">
    <p style="font-size:15px; font-weight:bold; margin:0 0 8px 0; color:#c62828;">⚠️ ご注意事項</p>
    <ul style="font-size:12px; color:#666; margin:0; padding-left:20px; line-height:1.8;">
      <li>海外発送商品のため、お届けまでに通常より日数がかかる場合があります</li>
      <li>パッケージデザインは予告なく変更される場合があります</li>
      <li>商品説明の一部に韓国語表記が含まれる場合があります</li>
      <li>個人輸入品として税関で課税される場合、お客様のご負担となります</li>
    </ul>
  </div>

  <!-- 문의 안내 -->
  <div style="text-align:center; padding:15px; background:#f5f5f5; border-radius:6px;">
    <p style="font-size:14px; font-weight:bold; color:#555; margin:0 0 5px 0;">💬 お問い合わせ</p>
    <p style="font-size:12px; color:#999; margin:0;">
      Qoo10メッセージにてお気軽にご連絡ください<br>
      営業時間：平日 10:00～18:00（韓国時間）
    </p>
    <p style="font-size:12px; color:#999; margin:8px 0 0 0;">
      Plan B Cabinet ｜ 韓国正規取引先からの直送品
    </p>
  </div>

</div>"""


def build_header_html(item: dict, analysis: dict = None, oliveyoung: dict = None) -> str:
    """
    상품별 header_html 생성.
    ★ v1.1: Python 코드가 HTML에 혼입되던 버그 수정
           marketing_html, social_proof_html을 f-string 밖에서 먼저 생성

    Args:
        item: 상품 정보 dict
        analysis: Gemini 분석 결과
        oliveyoung: 올리브영 검색 결과
    Returns:
        header HTML 문자열
    """
    try:
        from yakujiho_filter import sanitize_html
    except ImportError:
        def sanitize_html(h):
            return h

    name_jp = item.get("name_jp", item.get("name", ""))
    brand = item.get("brand", "")

    # 분석 결과 가져오기
    extracted = (analysis or {}).get("extracted", {})
    summary = (analysis or {}).get("summary_jp", {})
    oy = oliveyoung or {}

    volume = extracted.get("volume", "")
    ingredients = extracted.get("ingredients", [])
    headline = summary.get("headline", "")
    marketing_message = summary.get("marketing_message", "")
    points = summary.get("points", [])
    review_highlight = summary.get("review_highlight", "")
    texture_jp = summary.get("texture_jp", "")
    recommended = summary.get("recommended_for", [])
    usage_jp = summary.get("usage_jp", "")
    certifications = extracted.get("certifications", [])
    reviews_summary = extracted.get("reviews_summary", "")

    # ── 올리브영 배지 ──
    oy_badge = ""
    if oy.get("found"):
        ranking = oy.get("ranking", "")
        rating = oy.get("rating", 0)
        review_count = oy.get("review_count", 0)
        catchphrase = oy.get("catchphrase", "")

        oy_parts = []
        if ranking:
            oy_parts.append(f"🏆 韓国オリーブヤング {ranking}")
        if rating > 0:
            oy_parts.append(f"⭐ {rating}")
        if review_count > 0:
            oy_parts.append(f"レビュー {review_count:,}件")

        if oy_parts:
            catchphrase_line = ""
            if catchphrase:
                catchphrase_line = f'<p style="font-size:12px; margin:5px 0 0 0; color:#e8f5e9;">{catchphrase}</p>'
            oy_badge = f"""
  <div style="background:linear-gradient(135deg, #2e7d32, #43a047); padding:12px; border-radius:6px; margin-bottom:15px;">
    <p style="font-size:14px; margin:0; color:#fff; font-weight:bold;">
      {' ｜ '.join(oy_parts)}
    </p>
    {catchphrase_line}
  </div>"""

    # ── 인증 배지 ──
    cert_badge = ""
    if certifications:
        cert_tags = " ".join([
            f'<span style="display:inline-block; background:#e3f2fd; color:#1565c0; padding:3px 8px; border-radius:4px; font-size:11px; margin:2px;">{c}</span>'
            for c in certifications
        ])
        cert_badge = f"""
  <div style="margin-bottom:15px;">
    {cert_tags}
  </div>"""

    # ── 마케팅 메시지 (★ v1.1: f-string 밖에서 생성) ──
    marketing_html = ""
    if marketing_message:
        marketing_html = (
            '  <!-- 마케팅 메시지 -->\n'
            '  <div style="background:linear-gradient(135deg, #ff6b6b, #ee5a24); padding:14px; border-radius:6px; margin-bottom:15px;">\n'
            '    <p style="font-size:15px; margin:0; color:#fff; font-weight:bold;">\n'
            '      🔥 ' + marketing_message + '\n'
            '    </p>\n'
            '  </div>'
        )

    # ── 리뷰 하이라이트 + 사용감 (★ v1.1: f-string 밖에서 생성) ──
    social_proof_html = ""
    social_parts = []
    if review_highlight:
        social_parts.append('💬 "' + review_highlight + '"')
    if texture_jp:
        social_parts.append('🫧 ' + texture_jp)
    if reviews_summary and not review_highlight:
        social_parts.append('💬 ' + reviews_summary)

    if social_parts:
        items_str = ""
        for sp in social_parts:
            items_str += '    <p style="font-size:13px; color:#555; margin:5px 0;">' + sp + '</p>\n'
        social_proof_html = (
            '  <!-- 리뷰/사용감 -->\n'
            '  <div style="text-align:left; padding:15px; background:#f0f4f8; border-radius:6px; margin-bottom:15px; border-left:4px solid #5b9bd5;">\n'
            '    <p style="font-size:14px; font-weight:bold; margin:0 0 8px 0; color:#2c3e50;">📣 リアルな声</p>\n'
            + items_str
            + '  </div>'
        )

    # ── 포인트 리스트 ──
    points_html = ""
    if points:
        items_html = ""
        for p in points[:5]:
            items_html += f'      <li style="margin-bottom:5px;">{p}</li>\n'
        points_html = f"""
  <div style="text-align:left; padding:15px; background:#fdfbf9; border-radius:6px; margin-bottom:15px; border:1px solid #efe8dd;">
    <p style="font-size:15px; font-weight:bold; margin:0 0 10px 0; color:#c9a96e;">✨ この商品のポイント</p>
    <ul style="font-size:13px; color:#555; margin:0; padding-left:20px; line-height:1.8;">
{items_html}    </ul>
  </div>"""

    # ── 추천 대상 ──
    recommend_html = ""
    if recommended:
        rec_items = ""
        for r in recommended[:4]:
            rec_items += f'      <li style="margin-bottom:3px;">{r}</li>\n'
        recommend_html = f"""
  <div style="text-align:left; padding:15px; background:#f3e5f5; border-radius:6px; margin-bottom:15px;">
    <p style="font-size:15px; font-weight:bold; margin:0 0 10px 0; color:#7b1fa2;">💜 こんな方におすすめ</p>
    <ul style="font-size:13px; color:#555; margin:0; padding-left:20px; line-height:1.8;">
{rec_items}    </ul>
  </div>"""

    # ── 성분 정보 ──
    ingredients_html = ""
    if ingredients:
        ing_text = "、".join(ingredients[:5])
        ingredients_html = f"""
  <div style="text-align:left; padding:12px 15px; background:#e8eaf6; border-radius:6px; margin-bottom:15px;">
    <p style="font-size:13px; margin:0; color:#283593;">
      <strong>🧪 主な成分：</strong>{ing_text}
    </p>
  </div>"""

    # ── 사용법 ──
    usage_html = ""
    if usage_jp:
        usage_html = f"""
  <div style="text-align:left; padding:12px 15px; background:#fff3e0; border-radius:6px; margin-bottom:15px;">
    <p style="font-size:13px; margin:0; color:#e65100;">
      <strong>📝 使い方：</strong>{usage_jp}
    </p>
  </div>"""

    # ── 용량 텍스트 ──
    volume_text = f" {volume}" if volume else ""

    # ── 헤드라인 ──
    headline_html = ""
    if headline:
        headline_html = f'<p style="font-size:15px; color:#c9a96e; font-weight:bold; margin:10px 0 0 0;">{headline}</p>'

    # ══ 최종 HTML 조립 (★ v1.1: 순수 HTML만, Python 코드 없음) ══
    html = f"""<div style="text-align:center; font-family:'Hiragino Sans','Meiryo',sans-serif; max-width:750px; margin:0 auto; padding:10px; color:#333;">

  <!-- 브랜드 배너 -->
  <div style="background:linear-gradient(135deg, #1a1a1a, #333); padding:18px; border-radius:8px; margin-bottom:15px;">
    <p style="font-size:18px; margin:0 0 5px 0; color:#c9a96e; font-weight:bold;">Plan B Cabinet</p>
    <p style="font-size:12px; margin:0; color:#ccc;">韓国トレンドコスメ × プレミアムファッション</p>
  </div>

  <!-- LINE 유도 -->
  <div style="background:#06C755; padding:10px; border-radius:6px; margin-bottom:15px;">
    <p style="font-size:13px; margin:0; color:#fff; font-weight:bold;">
      📱 LINE友だち追加で200円クーポン → <span style="text-decoration:underline;">@planbasap</span>
    </p>
  </div>

{marketing_html}

{social_proof_html}

  <!-- 상품명 헤더 -->
  <div style="background:#fdfbf9; padding:20px; border-radius:8px; margin-bottom:15px; border:1px solid #efe8dd;">
    <h2 style="font-size:20px; margin:0 0 8px 0; color:#1a1a1a;">{name_jp}</h2>
    <p style="font-size:14px; color:#888; margin:5px 0;">🇰🇷 {brand}{volume_text} ｜ 韓国コスメ</p>
    {headline_html}
  </div>

  <!-- 정품보증 배너 -->
  <div style="background:linear-gradient(135deg, #c9a96e, #d4b87a); padding:12px; border-radius:6px; margin-bottom:15px;">
    <p style="font-size:14px; margin:0; color:#fff; font-weight:bold;">
      ✅ 正規品100%保証 ｜ 全品送料無料 ｜ 韓国直送 3〜7日
    </p>
  </div>

{oy_badge}
{cert_badge}
{points_html}
{recommend_html}
{ingredients_html}
{usage_html}

  <!-- 이미지 안내 -->
  <div style="padding:10px; margin-bottom:15px;">
    <p style="font-size:12px; color:#999;">▼ 商品の詳細ページ（韓国語原本）▼</p>
  </div>

  <!-- 구분선 -->
  <hr style="border:none; border-top:2px solid #efe8dd; margin:10px 0 20px 0;">

</div>"""

    # 약기법 필터 적용
    html = sanitize_html(html)

    return html


# ══════════════════════════════════════════════════════════════
# 5. 일괄 처리
# ══════════════════════════════════════════════════════════════

def analyze_and_build_html_batch(items: list) -> list:
    """
    상품 리스트를 일괄 분석하여 header_html, footer_html 생성.

    Args:
        items: 상품 리스트 (detail_images 필드 필요)
    Returns:
        items에 header_html, footer_html, analysis_data 필드 추가
    """
    client = _get_gemini_client()

    success_count = 0
    oy_match_count = 0

    for idx, item in enumerate(items):
        logger.info(f"[분석] {idx+1}/{len(items)} – {item.get('name', 'unknown')}")

        detail_images = item.get("detail_images", [])

        # 1) 이미지 분석
        analysis = _empty_analysis()
        if detail_images and client:
            analysis = analyze_product_images(detail_images, client)
            time.sleep(API_DELAY)

        # 2) 올리브영 검색
        oliveyoung = _empty_oliveyoung()
        brand = analysis.get("extracted", {}).get("brand", "") or item.get("brand", "")
        product_name = analysis.get("extracted", {}).get("product_name", "") or item.get("name", "")

        if brand and product_name and client:
            oliveyoung = search_oliveyoung(brand, product_name, client)
            time.sleep(API_DELAY)
            if oliveyoung.get("found"):
                oy_match_count += 1

        # 3) HTML 생성
        header = build_header_html(item, analysis, oliveyoung)
        item["header_html"] = header
        item["footer_html"] = FOOTER_HTML
        item["analysis_data"] = analysis
        item["oliveyoung_data"] = oliveyoung

        if analysis.get("extracted", {}).get("product_name"):
            success_count += 1

    logger.info(f"[분석 완료] 성공: {success_count}/{len(items)}건, 올리브영 매칭: {oy_match_count}건")
    return items


# ══════════════════════════════════════════════════════════════
# 테스트
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    test_item = {
        "name": "어성초 77% 수딩 토너",
        "name_jp": "【ANUA】ドクダミ77% スージングトナー 250ml",
        "brand": "ANUA",
        "detail_images": [
            "https://kmclubb2b.com/home/data/editor/2026/01/27/kbm-d5-1.jpg",
            "https://kmclubb2b.com/home/data/editor/2026/01/27/kbm-d5-2.jpg",
        ],
    }

    result = analyze_and_build_html_batch([test_item])
    print("\n=== Header HTML (앞 500자) ===")
    print(result[0].get("header_html", "")[:500])
    print("\n=== 분석 데이터 ===")
    print(json.dumps(result[0].get("analysis_data", {}), ensure_ascii=False, indent=2))
    print("\n=== 올리브영 데이터 ===")
    print(json.dumps(result[0].get("oliveyoung_data", {}), ensure_ascii=False, indent=2))
