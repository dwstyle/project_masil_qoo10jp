"""
thumbnail_processor.py – 썸네일 자동 생성 모듈
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 1.0

기능:
  - rembg로 배경 제거
  - 800×800 흰 배경 캔버스에 상품 배치
  - 일본어 배지 + 캐치프레이즈 텍스트 삽입
  - 올리브영 데이터 있으면 랭킹 배지 추가

비용: 무료 (rembg + Pillow 오픈소스)
"""

import io
import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── 설정 ──
CANVAS_SIZE = (800, 800)
PRODUCT_AREA = (600, 600)  # 상품 이미지 영역
PRODUCT_OFFSET = (100, 100)  # 상품 이미지 시작 위치
BG_COLOR = "#FFFFFF"
BADGE_COLOR = (201, 169, 110)  # #c9a96e 골드
BADGE_TEXT_COLOR = (255, 255, 255)
OY_BADGE_COLOR = (46, 125, 50)  # #2e7d32 올리브영 그린
BOTTOM_TEXT_COLOR = (51, 51, 51)

# 일본어 폰트 경로 (환경에 따라 조정)
FONT_PATHS = [
    "NotoSansJP-Bold.otf",
    "fonts/NotoSansJP-Bold.otf",
    "/usr/share/fonts/NotoSansJP-Bold.otf",
    os.path.expanduser("~/.fonts/NotoSansJP-Bold.otf"),
]

FONT_MEDIUM_PATHS = [
    "NotoSansJP-Medium.otf",
    "fonts/NotoSansJP-Medium.otf",
    "/usr/share/fonts/NotoSansJP-Medium.otf",
    os.path.expanduser("~/.fonts/NotoSansJP-Medium.otf"),
]


def _find_font(paths: list, fallback_size: int = 24):
    """사용 가능한 폰트 찾기"""
    try:
        from PIL import ImageFont
        for path in paths:
            if os.path.exists(path):
                return ImageFont.truetype(path, fallback_size)
        # 폰트 없으면 기본 폰트
        logger.warning("[폰트] 일본어 폰트 없음 – 기본 폰트 사용. NotoSansJP를 설치해주세요.")
        return ImageFont.load_default()
    except Exception:
        from PIL import ImageFont
        return ImageFont.load_default()


# ══════════════════════════════════════════════════════════════
# 1. 배경 제거
# ══════════════════════════════════════════════════════════════

def remove_background(image_bytes: bytes) -> Optional[bytes]:
    """
    rembg로 이미지 배경 제거.

    Args:
        image_bytes: 원본 이미지 바이트
    Returns:
        배경 제거된 PNG 바이트 (실패 시 None)
    """
    try:
        from rembg import remove
        result = remove(image_bytes)
        logger.debug("[배경제거] 성공")
        return result
    except ImportError:
        logger.error("rembg 미설치 – pip install rembg")
        return None
    except Exception as e:
        logger.error(f"[배경제거] 실패: {e}")
        return None


# ══════════════════════════════════════════════════════════════
# 2. 썸네일 생성
# ══════════════════════════════════════════════════════════════

def create_thumbnail(
    image_bytes: bytes,
    badge_text: str = "🇰🇷 韓国コスメ",
    bottom_text: str = "正規品保証 · 送料無料",
    oy_badge_text: str = "",
) -> Optional[bytes]:
    """
    최종 썸네일 생성.

    Args:
        image_bytes: 원본 이미지 바이트
        badge_text: 좌상단 배지 텍스트
        bottom_text: 하단 텍스트
        oy_badge_text: 올리브영 배지 (있으면 표시)
    Returns:
        800×800 JPG 바이트
    """
    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        logger.error("Pillow 미설치 – pip install Pillow")
        return None

    try:
        # 배경 제거
        no_bg_bytes = remove_background(image_bytes)
        if no_bg_bytes is None:
            # 배경 제거 실패 시 원본 사용
            no_bg_bytes = image_bytes

        # 이미지 로드
        product_img = Image.open(io.BytesIO(no_bg_bytes)).convert("RGBA")

        # 상품 이미지 리사이즈 (비율 유지)
        product_img.thumbnail(PRODUCT_AREA, Image.LANCZOS)
        pw, ph = product_img.size

        # 캔버스 생성
        canvas = Image.new("RGB", CANVAS_SIZE, BG_COLOR)

        # 상품 이미지 중앙 배치
        x = (CANVAS_SIZE[0] - pw) // 2
        y = (CANVAS_SIZE[1] - ph) // 2 - 20  # 약간 위로

        # RGBA → RGB 합성
        if product_img.mode == "RGBA":
            bg = Image.new("RGB", product_img.size, BG_COLOR)
            bg.paste(product_img, mask=product_img.split()[3])
            canvas.paste(bg, (x, y))
        else:
            canvas.paste(product_img, (x, y))

        # 드로잉
        draw = ImageDraw.Draw(canvas)
        font_bold = _find_font(FONT_PATHS, 24)
        font_medium = _find_font(FONT_MEDIUM_PATHS, 20)
        font_small = _find_font(FONT_MEDIUM_PATHS, 16)

        # ── 좌상단 배지 ──
        badge_w = min(len(badge_text) * 16 + 30, 350)
        draw.rounded_rectangle(
            [15, 15, badge_w, 52],
            radius=6,
            fill=BADGE_COLOR,
        )
        draw.text((25, 19), badge_text, font=font_bold, fill=BADGE_TEXT_COLOR)

        # ── 올리브영 배지 (있는 경우) ──
        if oy_badge_text:
            oy_w = min(len(oy_badge_text) * 16 + 30, 400)
            draw.rounded_rectangle(
                [15, 58, oy_w, 92],
                radius=6,
                fill=OY_BADGE_COLOR,
            )
            draw.text((25, 62), oy_badge_text, font=font_small, fill=BADGE_TEXT_COLOR)

        # ── 하단 텍스트 ──
        if bottom_text:
            bbox = draw.textbbox((0, 0), bottom_text, font=font_medium)
            text_w = bbox[2] - bbox[0]
            text_x = (CANVAS_SIZE[0] - text_w) // 2
            text_y = CANVAS_SIZE[1] - 50
            draw.text((text_x, text_y), bottom_text, font=font_medium, fill=BOTTOM_TEXT_COLOR)

        # JPG로 변환
        output = io.BytesIO()
        canvas.save(output, format="JPEG", quality=90)
        output.seek(0)

        logger.debug("[썸네일] 생성 성공")
        return output.getvalue()

    except Exception as e:
        logger.error(f"[썸네일] 생성 실패: {e}")
        return None


# ══════════════════════════════════════════════════════════════
# 3. 일괄 처리
# ══════════════════════════════════════════════════════════════

def process_thumbnails_batch(items: list) -> list:
    """
    상품 리스트의 썸네일을 일괄 처리.

    Args:
        items: 상품 리스트 (thumbnail, oliveyoung_data 필드)
    Returns:
        items에 thumbnail_processed 필드 추가
    """
    import requests

    success_count = 0

    for idx, item in enumerate(items):
        logger.info(f"[썸네일] {idx+1}/{len(items)} – {item.get('name', 'unknown')}")

        thumbnail_url = item.get("thumbnail", "") or item.get("image_url", "")
        if not thumbnail_url:
            item["thumbnail_processed"] = None
            continue

        # 이미지 다운로드
        try:
            resp = requests.get(thumbnail_url, timeout=15)
            if resp.status_code != 200:
                item["thumbnail_processed"] = None
                continue
            image_bytes = resp.content
        except Exception as e:
            logger.warning(f"[썸네일] 다운로드 실패: {e}")
            item["thumbnail_processed"] = None
            continue

        # 배지 텍스트 결정
        oy_data = item.get("oliveyoung_data", {})
        analysis = item.get("analysis_data", {})

        badge_text = "🇰🇷 韓国コスメ"
        bottom_text = "正規品保証 · 送料無料"
        oy_badge_text = ""

        if oy_data.get("found"):
            ranking = oy_data.get("ranking", "")
            if ranking:
                oy_badge_text = f"🏆 OY {ranking}"
            catchphrase = oy_data.get("catchphrase", "")
            if catchphrase:
                # 캐치프레이즈를 일본어로 (짧게)
                bottom_text = catchphrase[:20]
            rating = oy_data.get("rating", 0)
            review_count = oy_data.get("review_count", 0)
            if rating > 0:
                bottom_text += f" ⭐{rating}"

        # 인증 배지 추가
        certs = (analysis.get("extracted", {}) or {}).get("certifications", [])
        if certs:
            badge_text = " ".join(certs[:2])

        # 썸네일 생성
        result = create_thumbnail(image_bytes, badge_text, bottom_text, oy_badge_text)
        item["thumbnail_processed"] = result

        if result:
            success_count += 1

    logger.info(f"[썸네일 완료] {success_count}/{len(items)}건 생성")
    return items


# ══════════════════════════════════════════════════════════════
# 테스트
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    import requests

    test_url = "https://kmclubb2b.com/home/data/editor/2026/01/27/kbm-d5-1.jpg"
    resp = requests.get(test_url, timeout=15)

    if resp.status_code == 200:
        result = create_thumbnail(
            resp.content,
            badge_text="🇰🇷 韓国コスメ",
            bottom_text="正規品保証 · 送料無料",
            oy_badge_text="🏆 OY 保湿クリーム 1位",
        )
        if result:
            with open("test_thumbnail.jpg", "wb") as f:
                f.write(result)
            print("테스트 썸네일 생성: test_thumbnail.jpg")
        else:
            print("썸네일 생성 실패")
    else:
        print(f"이미지 다운로드 실패: {resp.status_code}")
