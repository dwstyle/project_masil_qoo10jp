"""
yakujiho_filter.py – 일본 약기법(薬機法) / 건강증진법 금지 키워드 필터
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 1.0

일본 약기법에서 화장품에 사용 금지된 표현을 자동 치환.
translator.py, product_analyzer.py, uploader_qoo10.py에서 공통 사용.
"""

import re
import logging

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# 1. 금지 키워드 → 대체 표현 매핑
# ══════════════════════════════════════════════════════════════

YAKUJIHO_REPLACEMENTS = {
    # --- 에이징 관련 ---
    "アンチエイジング": "エイジングケア※",
    "抗老化": "年齢に応じたケア",
    "若返り": "いきいきとした印象へ",
    "老化防止": "年齢に応じたお手入れ",
    "シワ改善": "乾燥による小ジワを目立たなくする",
    "シワ取り": "ハリを与える",
    "シワを消す": "ハリのある印象へ",
    "たるみ改善": "ハリのある印象へ",
    "たるみ解消": "引き締まった印象へ",
    "たるみを取る": "引き締まった印象へ",
    "リフトアップ": "ハリを与える",

    # --- 美白 関連 ---
    "ホワイトニング": "トーンアップ",
    "美白効果": "透明感を与える",
    "漂白": "トーンケア",
    "肌を白くする": "透明感のある肌へ",
    "シミを消す": "シミを防ぐ",
    "シミが消える": "シミを目立たなくする",
    "くすみを取る": "くすみをケアする",

    # --- 医療的表現 ---
    "治療": "ケア",
    "治す": "整える",
    "完治": "すこやかに保つ",
    "治癒": "すこやかに保つ",
    "再生": "すこやかな肌へ",
    "細胞再生": "肌をすこやかに保つ",
    "肌再生": "肌をすこやかに整える",
    "修復": "整える",
    "回復": "すこやかに保つ",

    # --- ニキビ・アトピー ---
    "ニキビを治す": "肌荒れを防ぐ",
    "ニキビが治る": "肌荒れを防ぐ",
    "ニキビ除去": "肌を清潔に保つ",
    "アトピー": "敏感肌向け",
    "アトピー改善": "敏感肌をやさしくケア",
    "アレルギー改善": "デリケートな肌をケア",

    # --- ダイエット・ボディ ---
    "痩せる": "スッキリとした印象",
    "ダイエット効果": "ボディケア",
    "脂肪燃焼": "めぐりサポート",
    "脂肪分解": "ボディケア",
    "セルライト除去": "ボディケア",
    "セルライト": "ボディケア",
    "部分痩せ": "ボディケア",

    # --- 殺菌・医薬 ---
    "除菌": "清潔に保つ",
    "殺菌": "清潔に保つ",
    "滅菌": "清潔に保つ",
    "抗菌効果": "清潔に保つ",
    "消毒": "清潔に保つ",
    "デトックス": "すっきりクリア",
    "毒素排出": "すっきりサポート",

    # --- 誇大表現 ---
    "100%効果": "しっかりケア",
    "即効性": "すばやくなじむ",
    "永久": "長時間",
    "万能": "マルチケア",
    "奇跡": "うれしい変化",
    "魔法": "うれしい変化",
    "完璧": "しっかりケア",
    "根本的に解決": "しっかりケア",
}

# 한국어 → 일본어 번역 전에 걸러야 할 한국어 키워드
YAKUJIHO_KR_REPLACEMENTS = {
    "안티에이징": "에이징케어",
    "주름개선": "건조로 인한 잔주름 케어",
    "주름제거": "탄력 케어",
    "미백": "톤업",
    "화이트닝": "톤업",
    "여드름치료": "피부결 케어",
    "아토피": "민감성 피부용",
    "지방분해": "바디케어",
    "셀룰라이트": "바디케어",
    "살균": "청결 케어",
    "소독": "청결 케어",
    "디톡스": "클린 케어",
}

# ※ 주석이 필요한 표현
FOOTNOTE_MARKERS = {"※"}
YAKUJIHO_FOOTNOTES = {
    "※": "※エイジングケアとは年齢に応じたお手入れのことです。",
}


# ══════════════════════════════════════════════════════════════
# 2. 필터 함수
# ══════════════════════════════════════════════════════════════

def sanitize_jp(text: str) -> tuple:
    """
    일본어 텍스트에서 약기법 금지 키워드를 대체 표현으로 치환.

    Args:
        text: 일본어 텍스트
    Returns:
        (치환된 텍스트, 필요한 주석 리스트, 치환된 키워드 수)
    """
    if not text:
        return text, [], 0

    footnotes = []
    replace_count = 0

    # 긴 키워드부터 먼저 치환 (부분 매칭 방지)
    sorted_keys = sorted(YAKUJIHO_REPLACEMENTS.keys(), key=len, reverse=True)

    for forbidden in sorted_keys:
        replacement = YAKUJIHO_REPLACEMENTS[forbidden]
        if forbidden in text:
            text = text.replace(forbidden, replacement)
            replace_count += 1
            logger.info(f"[약기법] '{forbidden}' → '{replacement}'")

            # 주석 필요 여부 확인
            for marker in FOOTNOTE_MARKERS:
                if marker in replacement and YAKUJIHO_FOOTNOTES[marker] not in footnotes:
                    footnotes.append(YAKUJIHO_FOOTNOTES[marker])

    return text, footnotes, replace_count


def sanitize_kr(text: str) -> str:
    """
    한국어 텍스트에서 약기법 위반 소지가 있는 키워드를 사전 치환.
    번역 전에 적용하여 번역 결과가 약기법을 위반하지 않도록 함.

    Args:
        text: 한국어 텍스트
    Returns:
        치환된 한국어 텍스트
    """
    if not text:
        return text

    sorted_keys = sorted(YAKUJIHO_KR_REPLACEMENTS.keys(), key=len, reverse=True)

    for forbidden in sorted_keys:
        replacement = YAKUJIHO_KR_REPLACEMENTS[forbidden]
        if forbidden in text:
            text = text.replace(forbidden, replacement)
            logger.info(f"[약기법-KR] '{forbidden}' → '{replacement}'")

    return text


def sanitize_html(html: str) -> str:
    """
    HTML 내 텍스트에서 약기법 금지 키워드를 치환.
    HTML 태그는 보존하고 텍스트만 처리.

    Args:
        html: HTML 문자열
    Returns:
        치환된 HTML
    """
    if not html:
        return html

    sanitized, footnotes, count = sanitize_jp(html)

    # 주석이 필요하면 HTML 끝에 추가
    if footnotes:
        footnote_html = '<div style="font-size:11px; color:#999; margin-top:15px; padding:10px; border-top:1px solid #eee;">'
        for fn in footnotes:
            footnote_html += f"<p style='margin:3px 0;'>{fn}</p>"
        footnote_html += "</div>"

        # </div> 마지막 태그 앞에 삽입
        if "</div>" in sanitized:
            last_div = sanitized.rfind("</div>")
            sanitized = sanitized[:last_div] + footnote_html + sanitized[last_div:]

    if count > 0:
        logger.info(f"[약기법] HTML 내 {count}건 키워드 치환 완료")

    return sanitized


def get_forbidden_keywords_found(text: str) -> list:
    """
    텍스트에 포함된 금지 키워드 목록 반환 (검사용).

    Args:
        text: 검사할 텍스트
    Returns:
        발견된 금지 키워드 리스트
    """
    found = []
    for keyword in YAKUJIHO_REPLACEMENTS:
        if keyword in text:
            found.append(keyword)
    return found


# ══════════════════════════════════════════════════════════════
# 테스트
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    # 일본어 필터 테스트
    test_jp = "このクリームはアンチエイジング効果があり、シワ改善とホワイトニングに最適です。"
    result, footnotes, count = sanitize_jp(test_jp)
    print(f"원본: {test_jp}")
    print(f"치환: {result}")
    print(f"주석: {footnotes}")
    print(f"치환 수: {count}")
    print()

    # 한국어 필터 테스트
    test_kr = "안티에이징 주름개선 미백 크림"
    result_kr = sanitize_kr(test_kr)
    print(f"원본(KR): {test_kr}")
    print(f"치환(KR): {result_kr}")
    print()

    # HTML 필터 테스트
    test_html = '<div style="color:#333;">アンチエイジングクリーム - シワ改善・ホワイトニング</div>'
    result_html = sanitize_html(test_html)
    print(f"HTML 치환:\n{result_html}")
