"""
crawler_kj.py – KJ9603 크롤링 모듈 (Qoo10 Japan Beauty 전용)
Project: Plan B Cabinet – Qoo10 Japan Beauty Sourcing
Version: 0.7

기존 project_masil/crawler.py v4.0 기반으로 재구성.
변경점:
  1. 키워드 검색 기능 추가 (PHASE B: 트렌드 키워드 → KJ9603 매칭)
  2. 상세 이미지 크롤링 추가 (detail_select1 img)
  3. 네이버 API 키 분리 (NAVER_CLIENT_ID_QOO10)
  4. 뷰티 카테고리 전용 필터링
"""

import os
import re
import time
import logging
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)

# ── 상수 ──────────────────────────────────────────────────────
BASE_URL       = "https://kj9603.xn--o39akkw25as0i.com"
LOGIN_URL      = f"{BASE_URL}/member/login.php"
SEARCH_URL     = f"{BASE_URL}/search.php"
PAGE_LOAD_WAIT = 10
KJ_ID          = os.getenv("KJ9603_ID", "")
KJ_PW          = os.getenv("KJ9603_PW", "")

# 큐텐 전용 네이버 API (기존 패션과 분리)
NAVER_CLIENT_ID     = os.getenv("NAVER_CLIENT_ID_QOO10", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET_QOO10", "")

KJ_DEFAULT_SHIPPING = 3_500

# 상세 이미지 셀렉터 (v0.7 확인)
DETAIL_IMAGE_SELECTOR = "div.item_mall_info_explain_wrap.detail_select1 img"


# ══════════════════════════════════════════════════════════════
# 1. WebDriver 세션 관리 (기존 동일)
# ══════════════════════════════════════════════════════════════

def get_session() -> webdriver.Chrome:
    """헤드리스 Chrome 드라이버 생성 + KJ9603 로그인"""
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    try:
        driver = webdriver.Chrome(options=opts)
    except Exception:
        from webdriver_manager.chrome import ChromeDriverManager
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)

    _login(driver)
    return driver


def close_driver(driver: webdriver.Chrome):
    """WebDriver 종료"""
    try:
        driver.quit()
        logger.info("WebDriver 종료 완료")
    except Exception as e:
        logger.warning(f"[close_driver] 종료 중 오류: {e}")


def _login(driver: webdriver.Chrome):
    """KJ9603 로그인"""
    try:
        driver.get(LOGIN_URL)
        _dismiss_alert(driver, timeout=3.0)

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
        _dismiss_alert(driver, timeout=3.0)

        if "login" not in driver.current_url:
            logger.info(f"KJ9603 로그인 성공: {driver.current_url}")
        else:
            body_text = driver.execute_script(
                "return document.body.innerText.substring(0, 200);"
            )
            logger.error(f"로그인 실패 | URL: {driver.current_url}")
            logger.error(f"  페이지 내용: {body_text}")

    except Exception as e:
        logger.error(f"로그인 오류: {e}")


# ══════════════════════════════════════════════════════════════
# 2. 공통 유틸 (기존 동일)
# ══════════════════════════════════════════════════════════════

def _dismiss_alert(driver, timeout: float = 3.0) -> bool:
    """JavaScript alert 감지 시 자동 수락"""
    try:
        WebDriverWait(driver, timeout).until(EC.alert_is_present())
        alert = driver.switch_to.alert
        logger.debug(f"[Alert] 감지 → 수락: {alert.text}")
        alert.accept()
        time.sleep(0.5)
        return True
    except Exception:
        return False


def _price_text_to_int(text: str) -> int:
    """가격 텍스트에서 숫자 추출"""
    nums = re.findall(r"[\d,]+", text)
    for n in nums:
        val = n.replace(",", "")
        if val.isdigit() and int(val) > 0:
            return int(val)
    return 0


def _parse_shipping_fee(text: str) -> int:
    """
    배송비 텍스트 파싱 (원)
    - "무료배송" → 0
    - "3,500원" → 3500
    - "10 개당 4,000원" → 4000
    - "착불" / 파싱 실패 → KJ_DEFAULT_SHIPPING
    """
    if not text or not text.strip():
        return KJ_DEFAULT_SHIPPING

    text = text.strip()

    if re.search(r"무료", text):
        return 0

    m = re.search(r"(\d+)\s*개당\s*([\d,]+)\s*원", text)
    if m:
        return int(m.group(2).replace(",", ""))

    m = re.search(r"([\d,]+)\s*원", text)
    if m:
        fee = int(m.group(1).replace(",", ""))
        if fee > 0:
            return fee

    return KJ_DEFAULT_SHIPPING


# ══════════════════════════════════════════════════════════════
# 3. 카테고리 상품 수집 (기존 동일)
# ══════════════════════════════════════════════════════════════

def _parse_category_page(driver, category_url: str = "") -> list:
    """
    mcategory.php 페이지 파싱 → 상품 목록 반환
    supply_price ← member4 (회원가), consumer_price ← cost4 (소비자정가)
    """
    soup = BeautifulSoup(driver.page_source, "lxml")

    product_tags = (
        soup.select("li.category_mall_list_item.category_mall_list_item4")
        or soup.select("li.category_mall_list_item")
    )

    if not product_tags:
        logger.warning(f"[parse] 상품 태그 0개 (URL: {category_url})")
        return []

    items = []
    for tag in product_tags:
        a_tag = (
            tag.select_one("div.category_mall_list_item_image a")
            or tag.select_one("a[href*='mitem.php']")
            or tag.select_one("a[href*='item=']")
        )
        if not a_tag:
            continue

        href = a_tag.get("href", "")
        m = re.search(r"item=(\d+)", href)
        if not m:
            continue

        item_id  = m.group(1)
        full_url = urljoin(BASE_URL, href)

        # 상품명
        name_tag = tag.select_one("div.category_mall_list_item_name4")
        if name_tag:
            name = name_tag.get("title", "").strip() or name_tag.get_text(strip=True)
        else:
            img_fb = tag.find("img")
            name = img_fb.get("alt", "").strip() if img_fb else ""

        # 썸네일
        img = tag.select_one("div.category_mall_list_item_image img") or tag.find("img")
        thumbnail = ""
        if img:
            thumbnail = (
                img.get("data-original", "")
                or img.get("data-src", "")
                or img.get("src", "")
            )

        # 가격
        cost_tag     = tag.select_one("p.category_mall_item_price_cost4")
        member_tag   = tag.select_one("p.category_mall_item_price_member4")
        cheapest_tag = tag.select_one("span.category_mall_item_price_cheapest4")

        consumer_price = _price_text_to_int(cost_tag.get_text())     if cost_tag     else 0
        supply_price   = _price_text_to_int(member_tag.get_text())   if member_tag   else 0
        cheapest_price = _price_text_to_int(cheapest_tag.get_text()) if cheapest_tag else 0

        if supply_price == 0 and consumer_price > 0:
            supply_price = consumer_price
            logger.warning(f"[parse] 회원가 0원 → 소비자가 폴백: {name[:30]} ({consumer_price:,}원)")

        # 배송비 (카테고리 페이지 임시값)
        shipping_inc = tag.select_one("span.category_mall_item_contain_shipping_price_4")
        kj_shipping  = 0 if shipping_inc else KJ_DEFAULT_SHIPPING

        items.append({
            "item_id":        item_id,
            "name":           name,
            "supply_price":   supply_price,
            "consumer_price": consumer_price,
            "cheapest_price": cheapest_price,
            "kj_shipping":    kj_shipping,
            "thumbnail":      thumbnail,
            "url":            full_url,
            "needs_detail":   (supply_price == 0),
        })

    return items


def get_category_products(driver, category_id: str, max_pages: int = 5) -> list:
    """카테고리별 상품 수집 (페이지네이션)"""
    all_items = []
    seen_ids  = set()

    for page in range(1, max_pages + 1):
        url = (
            f"{BASE_URL}/mcategory.php?category={category_id}"
            if page == 1
            else f"{BASE_URL}/mcategory.php?page={page}&category={category_id}"
        )

        try:
            driver.get(url)
            _dismiss_alert(driver, timeout=3.0)
            WebDriverWait(driver, PAGE_LOAD_WAIT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1.2)
            _dismiss_alert(driver, timeout=2.0)
        except Exception as e:
            logger.error(f"[get_category_products] 로드 실패 (id={category_id}, page={page}): {e}")
            break

        page_items = _parse_category_page(driver, url)
        if not page_items:
            break

        new_count = 0
        for item in page_items:
            if item["item_id"] not in seen_ids:
                seen_ids.add(item["item_id"])
                all_items.append(item)
                new_count += 1

        logger.info(f"[category] id={category_id} page={page} → 신규 {new_count}개 (누적 {len(all_items)}개)")
        if new_count == 0:
            break
        time.sleep(0.8)

    return all_items


# ══════════════════════════════════════════════════════════════
# 4. 키워드 검색 (신규 – PHASE B)
# ══════════════════════════════════════════════════════════════

def search_products(driver, keyword: str, max_pages: int = 3) -> list:
    """
    KJ9603 내부 검색으로 키워드 매칭 상품 수집
    Args:
        driver: 로그인된 WebDriver
        keyword: 검색어 (한국어 또는 영문 브랜드명)
        max_pages: 최대 페이지 수
    Returns:
        list of dict (상품 정보)
    """
    all_items = []
    seen_ids  = set()

    for page in range(1, max_pages + 1):
        if page == 1:
            url = f"{SEARCH_URL}?keyword={quote(keyword)}"
        else:
            url = f"{SEARCH_URL}?page={page}&keyword={quote(keyword)}"

        try:
            driver.get(url)
            _dismiss_alert(driver, timeout=3.0)
            WebDriverWait(driver, PAGE_LOAD_WAIT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(1.5)
            _dismiss_alert(driver, timeout=2.0)
        except Exception as e:
            logger.error(f"[search] 로드 실패 (keyword='{keyword}', page={page}): {e}")
            break

        # 검색 결과 파싱 (카테고리 페이지와 동일한 구조)
        page_items = _parse_category_page(driver, url)
        if not page_items:
            logger.debug(f"[search] keyword='{keyword}' page={page} → 결과 없음")
            break

        new_count = 0
        for item in page_items:
            if item["item_id"] not in seen_ids:
                seen_ids.add(item["item_id"])
                item["search_keyword"] = keyword
                all_items.append(item)
                new_count += 1

        logger.info(f"[search] keyword='{keyword}' page={page} → 신규 {new_count}개 (누적 {len(all_items)}개)")
        if new_count == 0:
            break
        time.sleep(1.0)

    return all_items


def search_by_trend_keywords(driver, sourcing_keywords: list, max_per_keyword: int = 3) -> list:
    """
    PHASE B: 트렌드 키워드 리스트로 KJ9603 일괄 검색
    Args:
        driver: 로그인된 WebDriver
        sourcing_keywords: trend_analyzer의 sourcing_keywords 리스트
        max_per_keyword: 키워드당 최대 페이지 수
    Returns:
        list of dict (모든 검색 결과 통합, 중복 제거)
    """
    all_items = []
    seen_ids  = set()

    for sk in sourcing_keywords:
        keyword_kr = sk.get("keyword_kr", "")

        # [TRANSLATE] 태그가 있으면 스킵 (번역 미완료)
        if not keyword_kr or keyword_kr.startswith("[TRANSLATE]"):
            logger.debug(f"[search_by_trend] 스킵: {sk.get('keyword_jp', '')} → {keyword_kr}")
            continue

        logger.info(f"[search_by_trend] 검색: '{keyword_kr}' (JP: {sk.get('keyword_jp', '')})")
        results = search_products(driver, keyword_kr, max_pages=max_per_keyword)

        new_count = 0
        for item in results:
            if item["item_id"] not in seen_ids:
                seen_ids.add(item["item_id"])
                item["trend_keyword_jp"] = sk.get("keyword_jp", "")
                item["trend_keyword_kr"] = keyword_kr
                item["demand_rank"]      = sk.get("demand_rank", 999)
                item["combined_score"]   = sk.get("combined_score", 0)
                all_items.append(item)
                new_count += 1

        logger.info(f"  → 신규 {new_count}개 (전체 누적 {len(all_items)}개)")
        time.sleep(0.5)

    logger.info(f"[search_by_trend] 총 {len(sourcing_keywords)}개 키워드 → {len(all_items)}개 상품 수집")
    return all_items


# ══════════════════════════════════════════════════════════════
# 5. 옵션 + 배송비 + 상세이미지 크롤링 (PHASE C)
# ══════════════════════════════════════════════════════════════

def fetch_item_detail(driver, item_url: str) -> dict:
    """
    상품 상세 페이지에서 옵션 + 배송비 + 상세이미지 추출

    기존 fetch_item_options()를 확장:
      - 옵션1/옵션2 파싱 (기존 동일)
      - 배송비 파싱 (기존 동일)
      - 상세 이미지 URL 수집 (신규)

    Returns:
        {
            "has_option":     True/False,
            "option1_name":   "색상",
            "option1_values": ["빨강", "파랑"],
            "option2_name":   "사이즈",
            "option2_values": ["S", "M", "L"],
            "shipping_fee":   4000,
            "shipping_text":  "10 개당 4,000원",
            "detail_images":  ["https://kmclubb2b.com/.../img1.jpg", ...],
        }
    """
    result = {
        "has_option":     False,
        "option1_name":   "",
        "option1_values": [],
        "option2_name":   "",
        "option2_values": [],
        "shipping_fee":   KJ_DEFAULT_SHIPPING,
        "shipping_text":  "",
        "detail_images":  [],
    }

    try:
        driver.get(item_url)
        _dismiss_alert(driver, timeout=3.0)
        WebDriverWait(driver, PAGE_LOAD_WAIT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(1.2)

        soup = BeautifulSoup(driver.page_source, "lxml")

        # ── 배송비 파싱 ──────────────────────────────
        try:
            shipping_span = soup.select_one("span.shipping_note")
            if shipping_span:
                shipping_text = shipping_span.get_text(strip=True)
                result["shipping_text"] = shipping_text
                result["shipping_fee"]  = _parse_shipping_fee(shipping_text)
            else:
                price_table = soup.select_one("table.item_mall_price_wrap")
                if price_table:
                    for row in price_table.select("tr"):
                        header_td = row.select_one("td.item_mall_price_header")
                        if header_td and "배송" in header_td.get_text():
                            content_td = row.select_one("td.item_mall_price_content")
                            if content_td:
                                shipping_text = content_td.get_text(strip=True)
                                result["shipping_text"] = shipping_text
                                result["shipping_fee"]  = _parse_shipping_fee(shipping_text)
                            break
        except Exception as e:
            logger.warning(f"[배송비 파싱 오류] {item_url[-30:]}: {e}")

        # ── 상세 이미지 수집 (신규) ──────────────────
        try:
            detail_imgs = soup.select(DETAIL_IMAGE_SELECTOR)
            for img in detail_imgs:
                src = img.get("src", "") or img.get("data-src", "") or img.get("data-original", "")
                if src and src.startswith("http"):
                    result["detail_images"].append(src)
            logger.info(f"[상세이미지] {len(result['detail_images'])}개 수집 ({item_url[-30:]})")
        except Exception as e:
            logger.warning(f"[상세이미지 오류] {item_url[-30:]}: {e}")

        # ── 옵션1 ────────────────────────────────────
        try:
            sel1_el = driver.find_element(By.CSS_SELECTOR, "select#item_option1")
            sel1    = Select(sel1_el)
            opt1_values = [
                o.text.strip()
                for o in sel1.options
                if o.text.strip() and "선택" not in o.text
            ]

            try:
                label_el = driver.find_element(By.CSS_SELECTOR, "label[for='item_option1']")
                opt1_name = label_el.text.strip()
            except Exception:
                opt1_name = sel1.options[0].text.strip().strip("=").strip() if sel1.options else "옵션1"

            result["option1_name"]   = opt1_name
            result["option1_values"] = opt1_values
            result["has_option"]     = bool(opt1_values)
        except Exception:
            return result

        # ── 옵션2 ────────────────────────────────────
        if opt1_values:
            try:
                sel1 = Select(driver.find_element(By.CSS_SELECTOR, "select#item_option1"))
                sel1.select_by_visible_text(opt1_values[0])
                time.sleep(1.5)

                sel2_el = driver.find_element(By.CSS_SELECTOR, "select#item_option2")
                sel2    = Select(sel2_el)
                opt2_values = [
                    o.text.strip()
                    for o in sel2.options
                    if o.text.strip() and "선택" not in o.text
                ]

                try:
                    label2_el = driver.find_element(By.CSS_SELECTOR, "label[for='item_option2']")
                    opt2_name = label2_el.text.strip()
                except Exception:
                    opt2_name = sel2.options[0].text.strip().strip("=").strip() if sel2.options else "옵션2"

                result["option2_name"]   = opt2_name
                result["option2_values"] = opt2_values
            except Exception:
                pass

    except Exception as e:
        logger.error(f"[fetch_item_detail] 오류 ({item_url}): {e}")

    return result


# ══════════════════════════════════════════════════════════════
# 6. 네이버 API (큐텐 전용 키)
# ══════════════════════════════════════════════════════════════

def get_naver_lowest_price(keyword: str) -> int:
    """네이버 쇼핑 최저가 검색"""
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        logger.warning("[naver_price] QOO10 전용 API 키 미설정")
        return 0

    url     = "https://openapi.naver.com/v1/search/shop.json"
    headers = {
        "X-Naver-Client-Id":     NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    params = {"query": keyword, "display": 5, "sort": "asc"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code == 429:
            logger.warning("[naver_price] Quota 초과 (429)")
            return 0
        if resp.status_code != 200:
            return 0

        items = resp.json().get("items", [])
        prices = []
        for item in items:
            try:
                prices.append(int(item.get("lprice", "0")))
            except ValueError:
                pass
        return min(prices) if prices else 0

    except Exception as e:
        logger.error(f"[naver_price] 오류: {e}")
        return 0


def get_naver_search_count(keyword: str) -> int:
    """네이버 쇼핑 검색 결과 수 (수요 지표)"""
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        return 0

    url     = "https://openapi.naver.com/v1/search/shop.json"
    headers = {
        "X-Naver-Client-Id":     NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    params = {"query": keyword, "display": 1}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code == 429:
            return -1
        if resp.status_code != 200:
            return 0
        return int(resp.json().get("total", 0))
    except Exception as e:
        logger.error(f"[naver_search_count] 오류: {e}")
        return 0


# ══════════════════════════════════════════════════════════════
# 7. PASS 1 필터링 (가격 범위 + 기본 조건)
# ══════════════════════════════════════════════════════════════

FILTER_MIN_SUPPLY_KRW = 5_000
FILTER_MAX_SUPPLY_KRW = 300_000

def pass1_filter(items: list) -> list:
    """
    PASS 1: 기본 필터링
    - 공급가 범위 (5,000 ~ 300,000 KRW)
    - 공급가 0원 제외
    - 중복 제거 (item_id 기준)
    Returns:
        필터 통과한 상품 리스트
    """
    seen_ids = set()
    filtered = []

    for item in items:
        iid = item.get("item_id", "")
        if iid in seen_ids:
            continue

        supply = item.get("supply_price", 0)
        if supply < FILTER_MIN_SUPPLY_KRW or supply > FILTER_MAX_SUPPLY_KRW:
            continue

        seen_ids.add(iid)
        filtered.append(item)

    logger.info(f"[PASS1] {len(items)}건 → {len(filtered)}건 통과")
    return filtered


# ══════════════════════════════════════════════════════════════
# 직접 실행 (테스트)
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    if not KJ_ID or not KJ_PW:
        print("ERROR: KJ9603_ID / KJ9603_PW 환경변수를 설정해주세요.")
        exit(1)

    print("crawler_kj.py 테스트 시작...")
    driver = get_session()

    try:
        # 테스트 1: 키워드 검색
        results = search_products(driver, "ANUA", max_pages=1)
        print(f"\n검색 결과 (ANUA): {len(results)}건")
        for item in results[:3]:
            print(f"  {item['name'][:40]} | 공급가: {item['supply_price']:,}원")

        # 테스트 2: 상세 페이지 크롤링
        if results:
            detail = fetch_item_detail(driver, results[0]["url"])
            print(f"\n상세 정보:")
            print(f"  배송비: {detail['shipping_fee']:,}원 ({detail['shipping_text']})")
            print(f"  상세이미지: {len(detail['detail_images'])}개")
            print(f"  옵션: {detail['has_option']}")

    finally:
        close_driver(driver)
        print("\n테스트 완료!")
