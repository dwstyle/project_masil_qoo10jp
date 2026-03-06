"""
crawler_kj.py – KJ9603 크롤러 (Qoo10 Japan Beauty Sourcing)
v0.9 – 2026-03-04
- 실제 KJ9603 URL 구조 반영 (search.php?search_category=mall&search_keyword=)
- kj9603_categories.json 로드 (13 중분류 + 73 소분류)
- 카테고리 전체 순회 + 키워드 검색 이중 전략
- 인기/추천/BEST 배지 가점
- KSE 배송 카테고리 자동 매핑
"""

import os
import re
import time
import json
import random
import logging
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════
BASE_URL = "https://kj9603.xn--o39akkw25as0i.com"
LOGIN_URL = f"{BASE_URL}/member/login.php"
SEARCH_URL = f"{BASE_URL}/search.php"
CATEGORY_URL = f"{BASE_URL}/mcategory.php"
ITEM_URL = f"{BASE_URL}/mitem.php"

PAGE_LOAD_WAIT = 10
KJ_DEFAULT_SHIPPING = 3500

# PASS 1 필터: 공급가 범위 (KRW)
PASS1_MIN_PRICE = 3000
PASS1_MAX_PRICE = 300000

# 상세 이미지 셀렉터
DETAIL_IMAGE_SELECTOR = "div.item_mall_info_explain_wrap img"

# Naver API (QOO10 전용)
NAVER_CLIENT_ID = os.environ.get("NAVER_CLIENT_ID_QOO10", "")
NAVER_CLIENT_SECRET = os.environ.get("NAVER_CLIENT_SECRET_QOO10", "")

# KJ9603 로그인 정보
KJ_ID = os.environ.get("KJ9603_ID", "")
KJ_PW = os.environ.get("KJ9603_PW", "")

# 인기/추천/BEST 배지 가점
BADGE_BONUS = {
    "best": 15,
    "인기": 12,
    "추천": 10,
    "hot": 10,
    "new": 5,
    "md추천": 12,
    "히트": 10,
}

# 한국어 검색 키워드 (KJ9603는 한국 도매몰)
SEARCH_KEYWORDS_KR = [
    # ── 브랜드명 ──
    "메디큐브", "쿤달", "VT", "라네즈", "이니스프리",
    "달바", "아누아", "클리오", "코스알엑스", "롬앤",
    "미샤", "티르티르", "네이처리퍼블릭", "바닐라코",
    "스킨푸드", "에뛰드", "토니모리", "홀리카홀리카",
    "마몽드", "헤라", "설화수", "CNP", "닥터지",
    "구달", "라운드랩", "아이소이", "정샘물", "루나",
    "조선미녀", "넘버즈", "메디힐", "JM솔루션",
    "비플레인", "아비브", "스킨1004", "믹순",
    # ── 카테고리 키워드 ──
    "토너", "세럼", "에센스", "크림", "로션",
    "선크림", "자외선차단", "클렌징", "폼클렌징",
    "마스크팩", "시트마스크", "수분크림",
    "쿠션", "파운데이션", "립스틱", "립틴트",
    "아이라이너", "마스카라", "아이섀도우",
    "샴푸", "트리트먼트", "바디워시", "바디로션",
    "핸드크림", "선스틱", "앰플", "미스트",
]


# ═══════════════════════════════════════════════
# 카테고리 JSON 로드
# ═══════════════════════════════════════════════
def _load_categories():
    """kj9603_categories.json 로드"""
    try:
        json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kj9603_categories.json")
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            logger.info(f"카테고리 JSON 로드 완료: {len(data.get('categories', {}))}개 중분류")
            return data
    except Exception as e:
        logger.warning(f"카테고리 JSON 로드 실패: {e} — 하드코딩 기본값 사용")
        return None


_CAT_DATA = _load_categories()

# 중분류 ID 리스트
BEAUTY_MID_CATEGORIES = (
    list(_CAT_DATA["categories"].keys()) if _CAT_DATA
    else ["680", "688", "695", "701", "706", "709", "713", "719",
          "725", "731", "739", "747", "752"]
)

# 우선 탐색 카테고리 (일본 수출 수요 높은 순)
PRIORITY_CATEGORIES = (
    _CAT_DATA.get("priority_categories", [680, 688, 695, 701, 709, 747]) if _CAT_DATA
    else [680, 688, 695, 701, 709, 747]
)


def get_kse_category(kj_category_id):
    """KJ9603 카테고리 ID → KSE 배송비 카테고리 매핑"""
    if not _CAT_DATA:
        return "default"
    cat_id = str(kj_category_id)
    for mid_id, mid_data in _CAT_DATA["categories"].items():
        children = mid_data.get("children", {})
        if cat_id in children:
            return children[cat_id].get("kse_category", "default")
        if cat_id == mid_id:
            if children:
                first = list(children.values())[0]
                return first.get("kse_category", "default")
    return "default"


def get_jp_name(kj_category_id):
    """KJ9603 카테고리 ID → 일본어 카테고리명"""
    if not _CAT_DATA:
        return ""
    cat_id = str(kj_category_id)
    for mid_id, mid_data in _CAT_DATA["categories"].items():
        if cat_id == mid_id:
            return mid_data.get("name_jp", "")
        children = mid_data.get("children", {})
        if cat_id in children:
            return children[cat_id].get("name_jp", "")
    return ""


def get_kr_name(kj_category_id):
    """KJ9603 카테고리 ID → 한국어 카테고리명"""
    if not _CAT_DATA:
        return ""
    cat_id = str(kj_category_id)
    for mid_id, mid_data in _CAT_DATA["categories"].items():
        if cat_id == mid_id:
            return mid_data.get("name_kr", "")
        children = mid_data.get("children", {})
        if cat_id in children:
            return children[cat_id].get("name_kr", "")
    return ""


# ═══════════════════════════════════════════════
# WebDriver & Session
# ═══════════════════════════════════════════════
_driver = None


def get_session():
    """헤드리스 Chrome 세션 생성 + KJ9603 로그인"""
    global _driver
    if _driver:
        return _driver

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0 Safari/537.36"
    )

    try:
        service = Service(ChromeDriverManager().install())
        _driver = webdriver.Chrome(service=service, options=options)
    except Exception:
        options.binary_location = "/usr/bin/chromium-browser"
        _driver = webdriver.Chrome(options=options)

    _driver.set_page_load_timeout(30)
    _login(_driver)
    return _driver


def _login(driver):
    """KJ9603 로그인"""
    try:
        driver.get(LOGIN_URL)
        time.sleep(2)
        _dismiss_alert(driver)

        # ID 입력
        id_field = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "input[name='id']")
            )
        )
        id_field.clear()
        id_field.send_keys(KJ_ID)

        pw_field = driver.find_element(
            By.CSS_SELECTOR,
            "input[name='pw']"
        )
        pw_field.clear()
        pw_field.send_keys(KJ_PW)

        # 로그인 버튼
        login_btn = driver.find_element(
            By.CSS_SELECTOR,
            "button[type='submit'], input[type='submit']"
        )
        login_btn.click()
        time.sleep(3)
        _dismiss_alert(driver)

        logger.info(f"KJ9603 로그인 성공: {driver.current_url}")
    except Exception as e:
        logger.error(f"KJ9603 로그인 실패: {e}")
        raise


def _dismiss_alert(driver):
    """팝업 알림 닫기"""
    try:
        alert = driver.switch_to.alert
        alert.accept()
    except Exception:
        pass


def close_driver():
    """WebDriver 종료"""
    global _driver
    if _driver:
        try:
            _driver.quit()
        except Exception:
            pass
        _driver = None


# ═══════════════════════════════════════════════
# 유틸리티
# ═══════════════════════════════════════════════
def _price_text_to_int(text):
    """'12,900원' → 12900"""
    if not text:
        return 0
    nums = re.sub(r'[^\d]', '', str(text))
    return int(nums) if nums else 0


def _parse_shipping_fee(text):
    """배송비 텍스트 → 정수 KRW"""
    if not text:
        return KJ_DEFAULT_SHIPPING
    text = text.strip()
    if "무료" in text or "free" in text.lower() or "0원" in text:
        return 0
    match = re.search(r'[\d,]+', text)
    if match:
        return _price_text_to_int(match.group())
    return KJ_DEFAULT_SHIPPING


# ═══════════════════════════════════════════════
# 배지 감지 & 가점
# ═══════════════════════════════════════════════
def _detect_badges(element):
    """상품 요소에서 인기/추천/BEST 배지 감지"""
    badges = []
    bonus = 0
    if not element:
        return badges, bonus

    try:
        item_text = element.get_text(separator=" ").lower()
        item_html = str(element).lower()

        for badge_key, badge_score in BADGE_BONUS.items():
            key_lower = badge_key.lower()
            if key_lower in item_text or key_lower in item_html:
                badges.append(badge_key)
                bonus = max(bonus, badge_score)
    except Exception:
        pass

    # 이미지 alt/src에서도 검색
    try:
        for img in element.find_all("img"):
            alt = (img.get("alt", "") or "").lower()
            src = (img.get("src", "") or "").lower()
            for badge_key, badge_score in BADGE_BONUS.items():
                key_lower = badge_key.lower()
                if key_lower in alt or key_lower in src:
                    if badge_key not in badges:
                        badges.append(badge_key)
                        bonus = max(bonus, badge_score)
    except Exception:
        pass

    return badges, bonus


# ═══════════════════════════════════════════════
# 상품 목록 파싱 (검색 & 카테고리 공용)
# ═══════════════════════════════════════════════
def _parse_product_list(soup, source_tag="search"):
    """HTML에서 상품 리스트 추출"""
    items = []

    # 셀렉터 후보 (KJ9603 구조)
    selectors = [
        "div.item_list_wrap li",
        "ul.item_list li",
        "div.goods_list li",
        "div.product_list li",
        ".item_cont",
        ".goods_item",
        "li.item",
        "div.item_box",
        "div.mall_list li",
        "ul.mall_goods_list li",
        "div.item_gallery_type li",
        "ul.item_gallery_type li",
    ]

    product_elements = []
    used_selector = ""
    for sel in selectors:
        product_elements = soup.select(sel)
        if product_elements:
            used_selector = sel
            break

    # 최후 시도: mitem.php 링크 기반
    if not product_elements:
        link_parents = set()
        for a_tag in soup.find_all("a", href=True):
            if "mitem.php?item=" in a_tag["href"]:
                parent = a_tag.parent
                if parent and parent not in link_parents:
                    link_parents.add(id(parent))
                    product_elements.append(parent)
        if product_elements:
            used_selector = "mitem.php links (fallback)"

    if product_elements:
        logger.info(f"  파싱 셀렉터: '{used_selector}' → {len(product_elements)}개")
    else:
        logger.warning(f"  [{source_tag}] 상품 태그 0개 — 셀렉터 매칭 실패")
        # 디버그: 페이지 내 mitem 링크 수 로깅
        mitem_links = soup.find_all("a", href=re.compile(r'mitem\.php\?item='))
        logger.debug(f"  mitem.php 링크 수: {len(mitem_links)}")

    seen_ids = set()
    for elem in product_elements:
        try:
            item = _extract_product_from_element(elem, source_tag)
            if item and item.get("product_id") and item["product_id"] not in seen_ids:
                seen_ids.add(item["product_id"])
                items.append(item)
        except Exception as e:
            logger.debug(f"  상품 파싱 오류: {e}")

    return items


def _extract_product_from_element(elem, source_tag):
    """개별 상품 요소 → 딕셔너리"""
    item = {"source": source_tag}

    # ── 링크 & ID ──
    link_tag = elem.find("a", href=True)
    if not link_tag:
        if elem.name == "a" and elem.get("href"):
            link_tag = elem
        else:
            return None

    href = link_tag["href"]
    match = re.search(r'item=(\d+)', href)
    if not match:
        return None

    item["product_id"] = match.group(1)
    if href.startswith("http"):
        item["url"] = href
    else:
        item["url"] = f"{BASE_URL}/{href.lstrip('/')}"

    # ── 상품명 ──
    name_selectors = [
        ".item_name", ".goods_name", ".prd_name", ".name",
        "p.name", "span.name", "div.name", "strong.name",
        ".item_tit", ".goods_tit", "h3", "h4",
        ".item_gallery_name", ".mall_item_name",
    ]
    name = ""
    for ns in name_selectors:
        tag = elem.select_one(ns)
        if tag:
            name = tag.get_text(strip=True)
            break
    if not name:
        img = elem.find("img")
        if img:
            name = img.get("alt", "").strip()
    if not name:
        name = link_tag.get_text(strip=True)
    item["name"] = name[:200] if name else f"상품_{item['product_id']}"

    # ── 가격은 상세 페이지에서만 파싱 ──
    item["supply_price"] = 0
    item["consumer_price"] = 0

    # ── 이미지 ──
    img_tag = elem.find("img")
    if img_tag:
        src = img_tag.get("src", "") or img_tag.get("data-src", "") or img_tag.get("data-original", "")
        if src and not src.startswith("http"):
            src = f"{BASE_URL}/{src.lstrip('/')}"
        item["image_url"] = src

    # ── 배지 ──
    badges, bonus = _detect_badges(elem)
    item["badges"] = badges
    item["badge_bonus"] = bonus

    return item


# ═══════════════════════════════════════════════
# 키워드 검색
# ═══════════════════════════════════════════════
def search_products(driver, keyword, max_pages=3):
    """KJ9603 키워드 검색 (한국어)"""
    all_items = []
    seen_ids = set()

    for page in range(1, max_pages + 1):
        try:
            url = (
                f"{SEARCH_URL}?search_category=mall"
                f"&search_keyword={keyword}&x=0&y=0"
            )
            if page > 1:
                url += f"&page={page}"

            driver.get(url)
            time.sleep(PAGE_LOAD_WAIT)
            _dismiss_alert(driver)

            soup = BeautifulSoup(driver.page_source, "lxml")
            items = _parse_product_list(soup, source_tag=f"search:{keyword}")

            new_count = 0
            for item in items:
                if item["product_id"] not in seen_ids:
                    seen_ids.add(item["product_id"])
                    item["search_keyword"] = keyword
                    all_items.append(item)
                    new_count += 1

            logger.info(f"  [{keyword}] p{page}: {new_count}건 (누적 {len(all_items)})")

            if new_count == 0:
                break

            time.sleep(random.uniform(2, 4))

        except Exception as e:
            logger.warning(f"  [{keyword}] p{page} 오류: {e}")
            break

    return all_items


# ═══════════════════════════════════════════════
# 카테고리 탐색
# ═══════════════════════════════════════════════
def browse_category(driver, category_id, category_name="", max_pages=3):
    """단일 카테고리 페이지 크롤링"""
    all_items = []
    seen_ids = set()

    for page in range(1, max_pages + 1):
        try:
            url = f"{CATEGORY_URL}?category={category_id}"
            if page > 1:
                url += f"&page={page}"

            driver.get(url)
            time.sleep(PAGE_LOAD_WAIT)
            _dismiss_alert(driver)

            soup = BeautifulSoup(driver.page_source, "lxml")
            items = _parse_product_list(soup, source_tag=f"category:{category_name or category_id}")

            new_count = 0
            for item in items:
                if item["product_id"] not in seen_ids:
                    seen_ids.add(item["product_id"])
                    item["category_id"] = category_id
                    item["category_name"] = category_name
                    item["kse_category"] = get_kse_category(category_id)
                    item["category_jp"] = get_jp_name(category_id)
                    all_items.append(item)
                    new_count += 1

            logger.info(f"  [카테고리:{category_name}] p{page}: {new_count}건 (누적 {len(all_items)})")

            if new_count == 0:
                break

            time.sleep(random.uniform(2, 4))

        except Exception as e:
            logger.warning(f"  [카테고리:{category_name}] p{page} 오류: {e}")
            break

    return all_items


def browse_all_beauty_categories(driver, max_pages_per_cat=2):
    """뷰티 전체 중분류 카테고리 순회 크롤링 (우선순위 순)"""
    all_items = []
    seen_ids = set()

    # 우선순위 카테고리 먼저
    ordered = [str(c) for c in PRIORITY_CATEGORIES]
    for mid_id in BEAUTY_MID_CATEGORIES:
        if mid_id not in ordered:
            ordered.append(mid_id)

    for cat_id in ordered:
        cat_data = _CAT_DATA["categories"].get(cat_id, {}) if _CAT_DATA else {}
        cat_name = cat_data.get("name_kr", cat_id)

        logger.info(f"─── 카테고리 탐색: {cat_name} (ID: {cat_id}) ───")
        items = browse_category(driver, int(cat_id), cat_name, max_pages=max_pages_per_cat)

        new_count = 0
        for item in items:
            pid = item.get("product_id", "")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_items.append(item)
                new_count += 1

        logger.info(f"  → {cat_name}: {new_count}건 신규 (누적 {len(all_items)})")
        time.sleep(random.uniform(2, 4))

    logger.info(f"═══ 전체 카테고리 탐색 완료: {len(all_items)}건 ═══")
    return all_items


# ═══════════════════════════════════════════════
# 인기/추천/BEST 상품 탐색
# ═══════════════════════════════════════════════
def browse_featured_products(driver):
    """인기/추천/BEST 정렬 페이지 탐색 → 가점 부여"""
    featured_items = []
    seen_ids = set()

    featured_urls = [
        (f"{CATEGORY_URL}?category=679&sort=popular", "beauty_popular", "인기", 12),
        (f"{CATEGORY_URL}?category=679&sort=recommend", "beauty_recommend", "추천", 10),
        (f"{CATEGORY_URL}?category=679&sort=best", "beauty_best", "best", 15),
        (f"{CATEGORY_URL}?category=679&order=hit", "beauty_hit", "인기", 12),
        (f"{CATEGORY_URL}?category=679&order=best", "beauty_best2", "best", 15),
        (f"{CATEGORY_URL}?category=679&order=review", "beauty_review", "인기", 10),
    ]

    for url, tag, badge_name, badge_score in featured_urls:
        try:
            driver.get(url)
            time.sleep(PAGE_LOAD_WAIT)
            _dismiss_alert(driver)

            soup = BeautifulSoup(driver.page_source, "lxml")
            items = _parse_product_list(soup, source_tag=tag)

            for item in items:
                pid = item.get("product_id", "")
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    item["badge_bonus"] = max(item.get("badge_bonus", 0), badge_score)
                    item.setdefault("badges", []).append(badge_name)
                    featured_items.append(item)

            logger.info(f"  [Featured:{tag}] {len(items)}건")
            time.sleep(random.uniform(1, 3))

        except Exception as e:
            logger.debug(f"  [Featured:{tag}] 접근 실패: {e}")

    logger.info(f"Featured 상품 총 {len(featured_items)}건 수집")
    return featured_items


# ═══════════════════════════════════════════════
# 트렌드 키워드 기반 일괄 검색
# ═══════════════════════════════════════════════
def search_by_trend_keywords(driver, sourcing_keywords=None, max_pages=2):
    """
    Phase A의 소싱 키워드 + 고정 키워드로 KJ9603 검색
    sourcing_keywords: [{"keyword": "...", "keyword_kr": "...", ...}, ...]
    """
    all_items = []
    seen_ids = set()

    # 한국어 검색어 수집
    kr_keywords = set()

    # 1) Phase A 소싱 키워드에서 한국어 추출
    if sourcing_keywords:
        for sk in sourcing_keywords:
            kr = sk.get("keyword_kr", "")
            if kr and kr != "[번역필요]":
                kr_keywords.add(kr)
            orig = sk.get("keyword", "")
            if orig and re.match(r'[가-힣]', orig):
                kr_keywords.add(orig)

    # 2) 고정 키워드 추가
    kr_keywords.update(SEARCH_KEYWORDS_KR)
    kr_keywords = sorted(list(kr_keywords))

    logger.info(f"KJ9603 키워드 검색 시작: {len(kr_keywords)}개 키워드")

    for i, kw in enumerate(kr_keywords):
        logger.info(f"  [{i+1}/{len(kr_keywords)}] 검색: '{kw}'")
        items = search_products(driver, kw, max_pages=max_pages)

        new_count = 0
        for item in items:
            pid = item.get("product_id", "")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_items.append(item)
                new_count += 1

        logger.info(f"  → '{kw}': {len(items)}건 중 {new_count}건 신규")
        time.sleep(random.uniform(1, 3))

    logger.info(f"키워드 검색 완료: {len(all_items)}건 (중복 제거)")
    return all_items


# ═══════════════════════════════════════════════
# PHASE B 통합 실행 (카테고리 + 키워드 + Featured)
# ═══════════════════════════════════════════════
def run_phase_b(driver, sourcing_keywords=None, max_cat_pages=2, max_search_pages=2):
    """
    PHASE B 전체 실행:
    1) 뷰티 카테고리 전체 순회
    2) 키워드 검색 (고정 + 트렌드)
    3) Featured (인기/추천/BEST) 가점 병합
    → 중복 제거된 전체 상품 리스트 반환
    """
    all_items = {}  # product_id → item (중복 제거용)

    # ── STEP 1: 카테고리 순회 ──
    logger.info("═══ PHASE B STEP 1: 카테고리 순회 ═══")
    cat_items = browse_all_beauty_categories(driver, max_pages_per_cat=max_cat_pages)
    for item in cat_items:
        pid = item.get("product_id", "")
        if pid:
            all_items[pid] = item
    logger.info(f"카테고리 순회: {len(cat_items)}건 수집")

    # ── STEP 2: 키워드 검색 ──
    logger.info("═══ PHASE B STEP 2: 키워드 검색 ═══")
    search_items = search_by_trend_keywords(driver, sourcing_keywords, max_pages=max_search_pages)
    new_from_search = 0
    for item in search_items:
        pid = item.get("product_id", "")
        if pid and pid not in all_items:
            all_items[pid] = item
            new_from_search += 1
        elif pid and pid in all_items:
            # 기존 아이템에 검색 키워드 추가
            existing = all_items[pid]
            if item.get("search_keyword"):
                existing.setdefault("search_keywords", []).append(item["search_keyword"])
    logger.info(f"키워드 검색: {len(search_items)}건 중 {new_from_search}건 신규 추가")

    # ── STEP 3: Featured 가점 병합 ──
    logger.info("═══ PHASE B STEP 3: Featured 가점 ═══")
    featured_items = browse_featured_products(driver)
    featured_count = 0
    for feat in featured_items:
        pid = feat.get("product_id", "")
        if pid and pid in all_items:
            # 기존 상품에 가점 병합
            existing = all_items[pid]
            existing["badge_bonus"] = max(
                existing.get("badge_bonus", 0),
                feat.get("badge_bonus", 0)
            )
            existing_badges = set(existing.get("badges", []))
            existing_badges.update(feat.get("badges", []))
            existing["badges"] = list(existing_badges)
            featured_count += 1
        elif pid and pid not in all_items:
            all_items[pid] = feat
    logger.info(f"Featured 가점 적용: {featured_count}건 병합")

    result = list(all_items.values())
    logger.info(f"═══ PHASE B 종합: {len(result)}건 (카테고리 {len(cat_items)} + 검색 신규 {new_from_search} + Featured 병합 {featured_count}) ═══")
    return result


# ═══════════════════════════════════════════════
# PASS 1 필터
# ═══════════════════════════════════════════════
def filter_pass1(items):
    """PASS 1: 기본 유효성 (가격은 상세 크롤링 후 price_calculator에서 판단)"""
    passed = []
    no_id = 0

    for item in items:
        if not item.get("product_id"):
            no_id += 1
            continue
        passed.append(item)

    logger.info(f"PASS 1: {len(items)}건 → {len(passed)}건 통과 (ID없음 {no_id})")
    return passed

# ═══════════════════════════════════════════════
# 상품 상세 페이지 크롤링
# ═══════════════════════════════════════════════
def fetch_item_detail(driver, item):
    """상세 페이지: 공급가 재확인, 배송비, 이미지, 옵션"""
    url = item.get("url", f"{ITEM_URL}?item={item['product_id']}")

    try:
        driver.get(url)
        _dismiss_alert(driver)
        
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "td.item_mall_price_content.item_mall_price_member"))
            )
        except:
            logger.warning(f"  회원가 요소 대기 시간 초과: {item.get('product_id')}")
        
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "lxml")

        # ── 공급가 재확인 ──
        for sel in ["p.category_mall_item_price_member4", ".member_price", ".member4"]:
            tag = soup.select_one(sel)
            if tag:
                val = _price_text_to_int(tag.get_text())
                if val > 0:
                    item["supply_price"] = val
                    break

        # ── 소비자가 ──
        for sel in ["p.category_mall_item_price_cost4", ".consumer_price", ".cost4"]:
            tag = soup.select_one(sel)
            if tag:
                val = _price_text_to_int(tag.get_text())
                if val > 0:
                    item["consumer_price"] = val
                    break

        # ── 배송비 ──
        shipping_text = ""
        for sel in ["span.shipping_note", "table.item_mall_price_wrap td.item_mall_price_content"]:
            tag = soup.select_one(sel)
            if tag:
                shipping_text = tag.get_text(strip=True)
                break
        item["shipping_fee"] = _parse_shipping_fee(shipping_text)
        item["shipping_text"] = shipping_text

        # ── 상세 이미지 ──
        detail_images = []
        for img in soup.select(DETAIL_IMAGE_SELECTOR):
            src = img.get("src", "") or img.get("data-src", "") or img.get("data-original", "")
            if src:
                if not src.startswith("http"):
                    src = f"{BASE_URL}/{src.lstrip('/')}"
                if src not in detail_images:
                    detail_images.append(src)
        item["detail_images"] = detail_images

        # ── 옵션 정보 ──
        options = []
        for opt in soup.select("select.item_option option, select[name*='option'] option"):
            opt_text = opt.get_text(strip=True)
            opt_val = opt.get("value", "")
            if opt_val and opt_text and opt_text not in ("선택", "- 선택 -", "선택하세요"):
                options.append({"text": opt_text, "value": opt_val})
        item["options"] = options

        # ── 배지 재확인 ──
        badges, bonus = _detect_badges(soup)
        if bonus > item.get("badge_bonus", 0):
            item["badge_bonus"] = bonus
            existing_badges = set(item.get("badges", []))
            existing_badges.update(badges)
            item["badges"] = list(existing_badges)

        logger.info(
            f"  상세: {item['name'][:30]} | "
            f"₩{item.get('supply_price', 0):,} | "
            f"배송 {item.get('shipping_fee', 0):,}원 | "
            f"이미지 {len(detail_images)}장 | "
            f"옵션 {len(options)}개 | "
            f"배지 {item.get('badges', [])}"
        )

    except Exception as e:
        logger.warning(f"  상세 크롤링 실패 [{item.get('product_id')}]: {e}")

    return item


def fetch_items_detail_batch(driver, items, limit=150):
    """상세 크롤링 배치 (상위 limit건)"""
    targets = items[:limit]
    logger.info(f"상세 크롤링 시작: {len(targets)}건 (전체 {len(items)}건 중)")

    for i, item in enumerate(targets):
        logger.info(f"  [{i+1}/{len(targets)}] {item.get('name', '')[:30]}")
        fetch_item_detail(driver, item)
        time.sleep(random.uniform(2, 4))

    logger.info(f"상세 크롤링 완료: {len(targets)}건")
    return targets


# ═══════════════════════════════════════════════
# Naver API 연동
# ═══════════════════════════════════════════════
def get_naver_lowest_price(product_name):
    """네이버 쇼핑 최저가 조회"""
    if not NAVER_CLIENT_ID:
        return None

    try:
        headers = {
            "X-Naver-Client-Id": NAVER_CLIENT_ID,
            "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
        }
        params = {"query": product_name, "display": 5, "sort": "asc"}
        resp = requests.get(
            "https://openapi.naver.com/v1/search/shop.json",
            headers=headers, params=params, timeout=5
        )
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("items", [])
            if items:
                prices = [int(i["lprice"]) for i in items if i.get("lprice")]
                return min(prices) if prices else None
    except Exception as e:
        logger.debug(f"  Naver API 오류: {e}")

    return None


def get_naver_search_count(product_name):
    """네이버 쇼핑 검색 결과 수"""
    if not NAVER_CLIENT_ID:
        return 0

    try:
        headers = {
            "X-Naver-Client-Id": NAVER_CLIENT_ID,
            "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
        }
        params = {"query": product_name, "display": 1}
        resp = requests.get(
            "https://openapi.naver.com/v1/search/shop.json",
            headers=headers, params=params, timeout=5
        )
        if resp.status_code == 200:
            return resp.json().get("total", 0)
    except Exception:
        pass

    return 0


# ═══════════════════════════════════════════════
# 메인 (테스트용)
# ═══════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    print("=" * 60)
    print("KJ9603 크롤러 테스트 (v0.9)")
    print("=" * 60)

    # 카테고리 맵 확인
    if _CAT_DATA:
        cats = _CAT_DATA["categories"]
        total_sub = sum(len(c.get("children", {})) for c in cats.values())
        print(f"\n카테고리: {len(cats)}개 중분류, {total_sub}개 소분류")
        print(f"우선순위: {PRIORITY_CATEGORIES}")
    else:
        print("\n카테고리 JSON 미로드 — 기본값 사용")

    # KSE 매핑 테스트
    test_ids = [682, 690, 710, 714, 748, 755]
    print("\nKSE 카테고리 매핑 테스트:")
    for tid in test_ids:
        print(f"  {tid} ({get_kr_name(tid)}) → KSE: {get_kse_category(tid)} | JP: {get_jp_name(tid)}")

    # 실제 크롤링 테스트 (로그인 필요)
    if KJ_ID and KJ_PW:
        driver = get_session()

        # 카테고리 테스트 (스킨케어 1페이지)
        print("\n── 카테고리 테스트: 스킨케어(680) ──")
        items = browse_category(driver, 680, "스킨케어", max_pages=1)
        print(f"결과: {len(items)}건")
        for item in items[:5]:
            print(f"  - {item.get('name', '')[:40]} | ₩{item.get('supply_price', 0):,} | {item.get('badges', [])}")

        # 검색 테스트
        print("\n── 검색 테스트: '토너' ──")
        items = search_products(driver, "토너", max_pages=1)
        print(f"결과: {len(items)}건")
        for item in items[:5]:
            print(f"  - {item.get('name', '')[:40]} | ₩{item.get('supply_price', 0):,}")

        close_driver()
    else:
        print("\nKJ9603 로그인 정보 없음 — 크롤링 테스트 스킵")

    print("\n테스트 완료!")
