# ══════════════════════════════════════════════════════════════
# crawler_kj.py — v1.0 (2026-03-06)
# KJ9603 뷰티 도매 크롤러 (Qoo10 JP 소싱용)
# 
# v0.9 → v1.0 변경사항:
#   1. --headless=new → --headless (v4.0 정렬)
#   2. 리스트 페이지 셀렉터: li.category_mall_list_item4 추가
#   3. 리스트 페이지 가격 파싱 복원 (member4/cost4)
#   4. 상세 페이지 가격은 보조 폴백 (item_mall_price_member + data-price)
#   5. 상세 페이지 배송비: span.shipping_note + 테이블 폴백
# ══════════════════════════════════════════════════════════════

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

# ══════════════════════════════════════════════════════════════
# 상수
# ══════════════════════════════════════════════════════════════
BASE_URL = "https://kj9603.xn--o39akkw25as0i.com"
LOGIN_URL = f"{BASE_URL}/member/login.php"
SEARCH_URL = f"{BASE_URL}/search.php"
CATEGORY_URL = f"{BASE_URL}/mcategory.php"
ITEM_URL = f"{BASE_URL}/mitem.php"

PAGE_LOAD_WAIT = 10
KJ_DEFAULT_SHIPPING = 3500

KJ_ID = os.getenv("KJ9603_ID", "")
KJ_PW = os.getenv("KJ9603_PW", "")

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID_QOO10", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET_QOO10", "")

# PASS 1 가격 필터 (리스트 페이지 기준)
PASS1_MIN_PRICE = 3000
PASS1_MAX_PRICE = 300000

# 배지 가점
BADGE_BONUS = {
    "best": 15, "베스트": 15,
    "인기": 12, "md추천": 12, "md": 12,
    "추천": 10, "hot": 10, "히트": 10,
    "new": 5, "신상": 5,
}

# 한국어 검색 키워드 (고정)
SEARCH_KEYWORDS_KR = [
    # 브랜드
    "메디큐브", "VT", "달바", "아누아", "토리든", "코스알엑스", "넘버즈인",
    "라운드랩", "이니스프리", "미샤", "에뛰드", "클리오", "롬앤", "바닐라코",
    "스킨푸드", "마녀공장", "아이소이", "정샘물", "헤라", "설화수",
    "비오힐보", "파티온", "티르티르", "조선미녀", "구달", "필리밀리",
    "일리윤", "CNP", "AHC", "닥터지", "에스트라", "셀퓨전씨",
    "더마비", "리얼배리어", "라네즈", "오휘", "수미37",
    # 카테고리
    "토너", "세럼", "앰플", "크림", "선크림", "클렌징", "마스크팩",
    "쿠션", "파운데이션", "립스틱", "아이라이너", "마스카라",
    "샴푸", "트리트먼트", "바디로션", "핸드크림",
    "에센스", "로션", "미스트", "오일", "올인원",
    "파우더", "프라이머", "컨실러", "블러셔", "아이섀도우",
    "립틴트", "립글로스", "립밤",
]

# ══════════════════════════════════════════════════════════════
# 카테고리 JSON 로드
# ══════════════════════════════════════════════════════════════
_CAT_DATA = None
BEAUTY_MID_CATEGORIES = []
PRIORITY_CATEGORIES = []

def _load_categories():
    global _CAT_DATA, BEAUTY_MID_CATEGORIES, PRIORITY_CATEGORIES
    try:
        json_path = os.path.join(os.path.dirname(__file__), "kj9603_categories.json")
        with open(json_path, "r", encoding="utf-8") as f:
            _CAT_DATA = json.load(f)
        BEAUTY_MID_CATEGORIES = list(_CAT_DATA.get("categories", {}).keys())
        PRIORITY_CATEGORIES = _CAT_DATA.get("priority_categories", [680, 688, 695, 701, 709, 747])
        logger.info(f"카테고리 JSON 로드 완료: {len(BEAUTY_MID_CATEGORIES)}개 중분류")
    except Exception as e:
        logger.warning(f"카테고리 JSON 로드 실패: {e}")
        BEAUTY_MID_CATEGORIES = ["680", "688", "695", "701", "706", "709", "713", "719", "725", "731", "739", "747", "752"]
        PRIORITY_CATEGORIES = [680, 688, 695, 701, 709, 747]

_load_categories()


def get_kse_category(category_id):
    if not _CAT_DATA:
        return "스킨케어"
    cat_id = str(category_id)
    for mid_id, mid_data in _CAT_DATA.get("categories", {}).items():
        if mid_id == cat_id:
            return mid_data.get("name_kr", "스킨케어")
        for sub_id, sub_data in mid_data.get("children", {}).items():
            if sub_id == cat_id:
                return sub_data.get("kse_category", mid_data.get("name_kr", "스킨케어"))
    return "스킨케어"


def get_jp_name(category_id):
    if not _CAT_DATA:
        return "スキンケア"
    cat_id = str(category_id)
    for mid_id, mid_data in _CAT_DATA.get("categories", {}).items():
        if mid_id == cat_id:
            return mid_data.get("name_jp", "スキンケア")
        for sub_id, sub_data in mid_data.get("children", {}).items():
            if sub_id == cat_id:
                return sub_data.get("name_jp", mid_data.get("name_jp", "スキンケア"))
    return "スキンケア"


def get_kr_name(category_id):
    if not _CAT_DATA:
        return "스킨케어"
    cat_id = str(category_id)
    for mid_id, mid_data in _CAT_DATA.get("categories", {}).items():
        if mid_id == cat_id:
            return mid_data.get("name_kr", cat_id)
        for sub_id, sub_data in mid_data.get("children", {}).items():
            if sub_id == cat_id:
                return sub_data.get("name_kr", mid_data.get("name_kr", cat_id))
    return cat_id


# ══════════════════════════════════════════════════════════════
# WebDriver & Session
# ══════════════════════════════════════════════════════════════
_driver = None


def get_session():
    """헤드리스 Chrome 세션 생성 + KJ9603 로그인"""
    global _driver
    if _driver:
        return _driver

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    try:
        service = Service(ChromeDriverManager().install())
        _driver = webdriver.Chrome(service=service, options=options)
    except Exception:
        _driver = webdriver.Chrome(options=options)

    _driver.set_page_load_timeout(30)
    _login(_driver)
    return _driver


def _login(driver):
    """KJ9603 로그인"""
    try:
        driver.get(LOGIN_URL)
        _dismiss_alert(driver)

        WebDriverWait(driver, PAGE_LOAD_WAIT).until(
            EC.presence_of_element_located((By.NAME, "id"))
        )
        time.sleep(0.5)

        id_el = driver.find_element(By.NAME, "id")
        pw_el = driver.find_element(By.NAME, "pw")
        id_el.clear()
        id_el.send_keys(KJ_ID)
        pw_el.clear()
        pw_el.send_keys(KJ_PW)

        btn = driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
        btn.click()

        time.sleep(2.5)
        _dismiss_alert(driver)

        if "login" not in driver.current_url:
            logger.info(f"KJ9603 로그인 성공: {driver.current_url}")
        else:
            logger.error(f"로그인 실패 | URL: {driver.current_url}")

    except Exception as e:
        logger.error(f"로그인 오류: {e}")


def close_driver():
    """WebDriver 종료"""
    global _driver
    if _driver:
        try:
            _driver.quit()
        except Exception:
            pass
        _driver = None
        logger.info("WebDriver 종료 완료")


# ══════════════════════════════════════════════════════════════
# 유틸리티
# ══════════════════════════════════════════════════════════════
def _dismiss_alert(driver):
    """팝업 알림 닫기"""
    try:
        WebDriverWait(driver, 3).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        alert.accept()
        time.sleep(0.5)
    except Exception:
        pass


def _price_text_to_int(text):
    """'12,900원' → 12900"""
    if not text:
        return 0
    nums = re.findall(r"[\d,]+", text)
    for n in nums:
        val = n.replace(",", "")
        if val.isdigit() and int(val) > 0:
            return int(val)
    return 0


def _parse_shipping_fee(text):
    """배송비 텍스트 → 정수 KRW"""
    if not text or not text.strip():
        return KJ_DEFAULT_SHIPPING
    text = text.strip()

    # 무료 패턴
    if re.search(r"무료", text):
        return 0

    # "N 개당 M,MMM원" 패턴
    m = re.search(r"(\d+)\s*개당\s*([\d,]+)\s*원", text)
    if m:
        return int(m.group(2).replace(",", ""))

    # "M,MMM원" 단순 금액
    m = re.search(r"([\d,]+)\s*원", text)
    if m:
        fee = int(m.group(1).replace(",", ""))
        if fee > 0:
            return fee

    return KJ_DEFAULT_SHIPPING


def _detect_badges(elem):
    """배지 키워드 감지 → (배지 리스트, 최대 보너스)"""
    badges = []
    bonus = 0
    if elem is None:
        return badges, bonus

    text = elem.get_text(separator=" ").lower() if hasattr(elem, 'get_text') else str(elem).lower()

    for keyword, point in BADGE_BONUS.items():
        if keyword in text:
            badges.append(keyword)
            bonus = max(bonus, point)

    return badges, bonus


# ══════════════════════════════════════════════════════════════
# 상품 목록 파싱 (검색 & 카테고리 공용)
# ══════════════════════════════════════════════════════════════
def _parse_product_list(soup, source_tag="search"):
    """HTML에서 상품 리스트 추출 (v4.0 셀렉터 정렬)"""
    items = []

    # 셀렉터 후보 (v4.0 우선)
    selectors = [
        "li.category_mall_list_item.category_mall_list_item4",
        "li.category_mall_list_item",
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
                if parent and id(parent) not in link_parents:
                    link_parents.add(id(parent))
                    product_elements.append(parent)
        if product_elements:
            used_selector = "mitem.php links (fallback)"

    if product_elements:
        logger.info(f"  파싱 셀렉터: '{used_selector}' → {len(product_elements)}개")
    else:
        logger.warning(f"  [{source_tag}] 상품 태그 0개 — 셀렉터 매칭 실패")
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
    """개별 상품 요소 → 딕셔너리 (v4.0 정렬)"""
    item = {"source": source_tag}

    # ── 링크 & ID ──
    link_tag = (
        elem.select_one("div.category_mall_list_item_image a")
        or elem.select_one("a[href*='mitem.php']")
        or elem.select_one("a[href*='item=']")
    )
    if not link_tag:
        if elem.name == "a" and elem.get("href"):
            link_tag = elem
        else:
            return None

    href = link_tag.get("href", "")
    match = re.search(r'item=(\d+)', href)
    if not match:
        return None

    item["product_id"] = match.group(1)
    if href.startswith("http"):
        item["url"] = href
    else:
        item["url"] = f"{BASE_URL}/{href.lstrip('/')}"

    # ── 상품명 (v4.0 정렬) ──
    name_tag = elem.select_one("div.category_mall_list_item_name4")
    if name_tag:
        name = name_tag.get("title", "").strip() or name_tag.get_text(strip=True)
    else:
        name_selectors = [
            ".item_name", ".goods_name", ".prd_name", ".name",
            "p.name", "span.name", "div.name", "strong.name",
            ".item_tit", ".goods_tit", "h3", "h4",
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

    # ── 가격 (v4.0 정렬: 리스트 페이지에서 파싱) ──
    member_tag = elem.select_one("p.category_mall_item_price_member4")
    cost_tag = elem.select_one("p.category_mall_item_price_cost4")
    cheapest_tag = elem.select_one("span.category_mall_item_price_cheapest4")

    item["supply_price"] = _price_text_to_int(member_tag.get_text()) if member_tag else 0
    item["consumer_price"] = _price_text_to_int(cost_tag.get_text()) if cost_tag else 0
    item["cheapest_price"] = _price_text_to_int(cheapest_tag.get_text()) if cheapest_tag else 0

    # 회원가 0 → 소비자가 폴백
    if item["supply_price"] == 0 and item["consumer_price"] > 0:
        item["supply_price"] = item["consumer_price"]
        logger.warning(f"  회원가 0원 → 소비자가 폴백: {item['name'][:30]}")

    # ── 썸네일 (v4.0 정렬) ──
    img_tag = elem.select_one("div.category_mall_list_item_image img") or elem.find("img")
    if img_tag:
        src = (
            img_tag.get("data-original", "")
            or img_tag.get("data-src", "")
            or img_tag.get("src", "")
        )
        if src and not src.startswith("http"):
            src = f"{BASE_URL}/{src.lstrip('/')}"
        item["image_url"] = src
    else:
        item["image_url"] = ""

    # ── 배송비 (리스트 임시값) ──
    shipping_inc = elem.select_one("span.category_mall_item_contain_shipping_price_4")
    item["shipping_fee"] = 0 if shipping_inc else KJ_DEFAULT_SHIPPING

    # ── 배지 ──
    badges, bonus = _detect_badges(elem)
    item["badges"] = badges
    item["badge_bonus"] = bonus

    return item


# ══════════════════════════════════════════════════════════════
# 키워드 검색
# ══════════════════════════════════════════════════════════════
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
            _dismiss_alert(driver)
            WebDriverWait(driver, PAGE_LOAD_WAIT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1.2)
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

            time.sleep(random.uniform(1, 3))

        except Exception as e:
            logger.warning(f"  [{keyword}] p{page} 오류: {e}")
            break

    return all_items


# ══════════════════════════════════════════════════════════════
# 카테고리 탐색
# ══════════════════════════════════════════════════════════════
def browse_category(driver, category_id, category_name="", max_pages=3):
    """단일 카테고리 페이지 크롤링 (v4.0 정렬)"""
    all_items = []
    seen_ids = set()

    for page in range(1, max_pages + 1):
        try:
            url = f"{CATEGORY_URL}?category={category_id}"
            if page > 1:
                url = f"{CATEGORY_URL}?page={page}&category={category_id}"

            driver.get(url)
            _dismiss_alert(driver)
            WebDriverWait(driver, PAGE_LOAD_WAIT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1.2)
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

            time.sleep(random.uniform(1, 2))

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
        time.sleep(random.uniform(1, 3))

    logger.info(f"═══ 전체 카테고리 탐색 완료: {len(all_items)}건 ═══")
    return all_items


# ══════════════════════════════════════════════════════════════
# Featured 상품 (인기/추천/BEST) 가점
# ══════════════════════════════════════════════════════════════
def browse_featured_products(driver):
    """인기/추천/BEST 상품 수집 → 배지 가점 적용"""
    featured = []
    seen_ids = set()

    featured_urls = [
        (f"{BASE_URL}/mcategory.php?category=679&order=hit", "인기"),
        (f"{BASE_URL}/mcategory.php?category=679&order=best", "best"),
        (f"{BASE_URL}/mcategory.php?category=679&order=review", "추천"),
    ]

    for url, badge_name in featured_urls:
        try:
            driver.get(url)
            _dismiss_alert(driver)
            WebDriverWait(driver, PAGE_LOAD_WAIT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1.2)
            _dismiss_alert(driver)

            soup = BeautifulSoup(driver.page_source, "lxml")
            items = _parse_product_list(soup, source_tag=f"featured:{badge_name}")

            for item in items:
                pid = item.get("product_id", "")
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    # 배지 추가
                    item_badges = item.get("badges", [])
                    if badge_name not in item_badges:
                        item_badges.append(badge_name)
                    item["badges"] = item_badges
                    item["badge_bonus"] = max(
                        item.get("badge_bonus", 0),
                        BADGE_BONUS.get(badge_name, 0)
                    )
                    featured.append(item)

            logger.info(f"  Featured [{badge_name}]: {len(items)}건")
            time.sleep(random.uniform(1, 2))

        except Exception as e:
            logger.warning(f"  Featured [{badge_name}] 오류: {e}")

    logger.info(f"Featured 상품 총 {len(featured)}건 수집")
    return featured


# ══════════════════════════════════════════════════════════════
# 트렌드 키워드 기반 일괄 검색
# ══════════════════════════════════════════════════════════════
def search_by_trend_keywords(driver, sourcing_keywords=None, max_pages=2):
    """Phase A 소싱 키워드 + 고정 키워드로 KJ9603 검색"""
    all_items = []
    seen_ids = set()

    kr_keywords = set()

    # Phase A 소싱 키워드에서 한국어 추출
    if sourcing_keywords:
        for sk in sourcing_keywords:
            kr = sk.get("keyword_kr", "")
            if kr and kr != "[번역필요]":
                kr_keywords.add(kr)
            orig = sk.get("keyword", "")
            if orig and re.match(r'[가-힣]', orig):
                kr_keywords.add(orig)

    # 고정 키워드 추가
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


# ══════════════════════════════════════════════════════════════
# PHASE B 통합 실행
# ══════════════════════════════════════════════════════════════
def run_phase_b(driver, sourcing_keywords=None, max_cat_pages=2, max_search_pages=2):
    """
    PHASE B 전체:
    1) 뷰티 카테고리 순회
    2) 키워드 검색
    3) Featured 가점 병합
    → 중복 제거된 전체 상품 리스트
    """
    all_items = {}

    # STEP 1: 카테고리 순회
    logger.info("═══ PHASE B STEP 1: 카테고리 순회 ═══")
    cat_items = browse_all_beauty_categories(driver, max_pages_per_cat=max_cat_pages)
    for item in cat_items:
        pid = item.get("product_id", "")
        if pid:
            all_items[pid] = item
    logger.info(f"카테고리 순회: {len(cat_items)}건 수집")

    # STEP 2: 키워드 검색
    logger.info("═══ PHASE B STEP 2: 키워드 검색 ═══")
    search_items = search_by_trend_keywords(driver, sourcing_keywords, max_pages=max_search_pages)
    new_from_search = 0
    for item in search_items:
        pid = item.get("product_id", "")
        if pid and pid not in all_items:
            all_items[pid] = item
            new_from_search += 1
        elif pid and pid in all_items:
            existing = all_items[pid]
            if item.get("search_keyword"):
                existing.setdefault("search_keywords", []).append(item["search_keyword"])
    logger.info(f"키워드 검색: {len(search_items)}건 중 {new_from_search}건 신규 추가")

    # STEP 3: Featured 가점 병합
    logger.info("═══ PHASE B STEP 3: Featured 가점 ═══")
    featured_items = browse_featured_products(driver)
    featured_count = 0
    for feat in featured_items:
        pid = feat.get("product_id", "")
        if pid and pid in all_items:
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
    logger.info(
        f"═══ PHASE B 종합: {len(result)}건 "
        f"(카테고리 {len(cat_items)} + 검색 신규 {new_from_search} + Featured 병합 {featured_count}) ═══"
    )
    return result


# ══════════════════════════════════════════════════════════════
# PASS 1 필터
# ══════════════════════════════════════════════════════════════
def filter_pass1(items):
    """PASS 1: 중복 제거 + 기본 유효성"""
    passed = []
    no_id = 0
    for item in items:
        if not item.get("product_id"):
            no_id += 1
            continue
        passed.append(item)

    logger.info(f"PASS 1: {len(items)}건 → {len(passed)}건 통과 (ID없음 {no_id})")
    return passed


# ══════════════════════════════════════════════════════════════
# 상품 상세 페이지 크롤링
# ══════════════════════════════════════════════════════════════
def fetch_item_detail(driver, item):
    """
    상세 페이지: 배송비, 이미지, 옵션, 가격 재확인(폴백)
    v4.0 정렬: 가격은 리스트에서 이미 파싱, 여기서는 보조 재확인
    """
    url = item.get("url", f"{ITEM_URL}?item={item['product_id']}")

    try:
        driver.get(url)
        _dismiss_alert(driver)
        WebDriverWait(driver, PAGE_LOAD_WAIT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(1.0)
        _dismiss_alert(driver)

        soup = BeautifulSoup(driver.page_source, "lxml")

        # ── 공급가 재확인 (리스트에서 0이었을 때만) ──
        if item.get("supply_price", 0) <= 0:
            # 방법 1: td.item_mall_price_member
            tag = soup.select_one("td.item_mall_price_content.item_mall_price_member")
            if tag:
                val = _price_text_to_int(tag.get_text())
                if val > 0:
                    item["supply_price"] = val
                    logger.info(f"  상세 가격 보정: ₩{val:,}")

            # 방법 2: data-price 속성 폴백
            if item.get("supply_price", 0) <= 0:
                input_tag = soup.select_one("input.item_mall_volume[data-price]")
                if input_tag:
                    val = int(input_tag.get("data-price", "0"))
                    if val > 0:
                        item["supply_price"] = val
                        logger.info(f"  data-price 폴백: ₩{val:,}")

        # ── 소비자가 재확인 (없을 때만) ──
        if item.get("consumer_price", 0) <= 0:
            tag = soup.select_one("td.item_mall_price_content.item_mall_price_cost")
            if tag:
                val = _price_text_to_int(tag.get_text())
                if val > 0:
                    item["consumer_price"] = val

        # ── 최종 폴백: supply 0이면 consumer 사용 ──
        if item.get("supply_price", 0) <= 0 and item.get("consumer_price", 0) > 0:
            item["supply_price"] = item["consumer_price"]
            logger.warning(f"  회원가 없음 → 소비자가 폴백: ₩{item['supply_price']:,}")

        # ── 배송비 (v4.0 정렬) ──
        try:
            # 방법 1: span.shipping_note
            shipping_span = soup.select_one("span.shipping_note")
            if shipping_span:
                shipping_text = shipping_span.get_text(strip=True)
                item["shipping_fee"] = _parse_shipping_fee(shipping_text)
                item["shipping_text"] = shipping_text
            else:
                # 방법 2: 테이블에서 "배송" 행 찾기
                price_table = soup.select_one("table.item_mall_price_wrap")
                if price_table:
                    for row in price_table.select("tr"):
                        header_td = row.select_one("td.item_mall_price_header")
                        if header_td and "배송" in header_td.get_text():
                            content_td = row.select_one("td.item_mall_price_content")
                            if content_td:
                                shipping_text = content_td.get_text(strip=True)
                                item["shipping_fee"] = _parse_shipping_fee(shipping_text)
                                item["shipping_text"] = shipping_text
                            break
        except Exception as e:
            logger.warning(f"  배송비 파싱 오류: {e}")

        # ── 상세 이미지 ──
        detail_images = []
        for img in soup.select("div.item_mall_info_explain_wrap img"):
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
        time.sleep(random.uniform(1, 3))

    logger.info(f"상세 크롤링 완료: {len(targets)}건")
    return targets


# ══════════════════════════════════════════════════════════════
# 네이버 API
# ══════════════════════════════════════════════════════════════
def get_naver_lowest_price(keyword, client_id="", client_secret=""):
    _id = client_id or NAVER_CLIENT_ID
    _secret = client_secret or NAVER_CLIENT_SECRET

    if not _id or not _secret:
        return 0

    url = "https://openapi.naver.com/v1/search/shop.json"
    headers = {"X-Naver-Client-Id": _id, "X-Naver-Client-Secret": _secret}
    params = {"query": keyword, "display": 5, "sort": "asc"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code != 200:
            return 0
        items = resp.json().get("items", [])
        prices = [int(i.get("lprice", "0")) for i in items if int(i.get("lprice", "0")) > 0]
        return min(prices) if prices else 0
    except Exception as e:
        logger.error(f"[naver_price] 오류: {e}")
        return 0


def get_naver_search_count(keyword, client_id="", client_secret=""):
    _id = client_id or NAVER_CLIENT_ID
    _secret = client_secret or NAVER_CLIENT_SECRET

    if not _id or not _secret:
        return 0

    url = "https://openapi.naver.com/v1/search/shop.json"
    headers = {"X-Naver-Client-Id": _id, "X-Naver-Client-Secret": _secret}
    params = {"query": keyword, "display": 1}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code != 200:
            return 0
        return int(resp.json().get("total", 0))
    except Exception as e:
        logger.error(f"[naver_search_count] 오류: {e}")
        return 0


# ══════════════════════════════════════════════════════════════
# 메인 (테스트용)
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    logger.info("=== crawler_kj.py v1.0 테스트 ===")
    logger.info(f"카테고리: {len(BEAUTY_MID_CATEGORIES)}개 중분류")
    logger.info(f"우선순위: {PRIORITY_CATEGORIES}")
    logger.info(f"검색 키워드: {len(SEARCH_KEYWORDS_KR)}개")

    # KSE 매핑 테스트
    test_ids = [680, 684, 688, 695, 701, 709, 747, 752]
    for tid in test_ids:
        logger.info(f"  {tid} → KSE: {get_kse_category(tid)}, JP: {get_jp_name(tid)}, KR: {get_kr_name(tid)}")

    if KJ_ID and KJ_PW:
        logger.info("로그인 정보 확인됨 — 크롤링 테스트 가능")
    else:
        logger.warning("KJ9603_ID/PW 미설정 — 크롤링 테스트 불가")
