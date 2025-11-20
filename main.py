import json
import logging
import os
import re
import shutil
from typing import List, Dict, Optional, TypedDict

import requests
import urllib3
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =======================
#      CONFIG & LOGGING
# =======================

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)

TARGET_URL = "https://www.gdt.gov.vn/wps/portal/!ut/p/z1/tZNNc4IwEIb_SnvwyGRTEj6O6LSCgzrWQSQXJwSlaQW0ZujHrxdqD60dxY6ay85mdp9s3rxBDE0Ry3kpU65kkfNllUfMmAGxPd-ajIc67RPwcAd6fc_CXY-g8Kug03VcYvoAFukCeKQ9HLidEQZPR-xn_yPVH8ALBq49HPkYKHz3w4HlQFP_BDHERK5W6glFaaJuRJGrea5asOGzKq-jJsWmBUoIkZQtsDA1zJhyLY5hoRE-x5qNY7tKbWLHdwblPKmhKyETFJ1UHf5W6e-U7BQRjxSw4xqF9bwNz9DEiKoZzMN3wCgs5fwNBXnxmlXGGP9TIrfxBPPME47jfXJdPD0T30MsXRbx7svJ5_WaOZWvay-_KzS9nLH3rrFntZFxWZX28fSqeNe8Kt4-10KrLAgyS_-Q2suif6-TqFd-tgdaHfx6g6bZbBec2y3wS1r1/dz/d5/L2dBISEvZ0FBIS9nQSEh/"
HDDTVN_FILE = "hddtvn.json"
DATE_FILE = "date.txt"

# Regex Patterns
TAX_ID_PATTERN = re.compile(r"(?i)(?:MST|MS|MST số)[\s:.]*(\d+)")
CLEAN_NAME_PATTERN = re.compile(r"\s*[(\[]?(?:MST|MS|MST số)[\s:.]*\d+[])]?", re.IGNORECASE)
DATE_PATTERN = re.compile(r"(\d{2}/\d{2})\s*/?\s*(\d{4})")


# Type Definition
class TaxRecord(TypedDict):
    stt: str
    ten_to_chuc: str
    mst: str
    dia_chi: str
    trang_thong_tin: str


# =======================
#      CORE CLASS
# =======================

class GDTScraper:
    def __init__(self):
        self.session = self._create_session()

    @staticmethod
    def _create_session() -> requests.Session:
        session = requests.Session()

        # --- 1. PROXY CONFIGURATION ---
        # Automatically retrieves PROXY_URL from environment variables (GitHub Secrets)
        proxy_url = os.environ.get("PROXY_URL")

        if proxy_url:
            session.proxies = {
                "http": proxy_url,
                "https": proxy_url
            }
            logger.info("🛡️ Proxy configuration loaded from environment.")
        else:
            logger.info("ℹ️ No PROXY_URL found. Running Direct Connection.")
        # ------------------------------

        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        })

        # Retry Strategy
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )

        # Use Standard HTTPAdapter
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def fetch_html(self, url: str) -> Optional[str]:
        try:
            logger.info(f"🌐 Fetching URL: {url[:60]}...")

            # verify=False is STILL REQUIRED for this site (even without legacy adapter)
            r = self.session.get(url, timeout=30, verify=False)
            r.raise_for_status()
            return r.text

        except requests.exceptions.ProxyError:
            logger.error("❌ Proxy Error: Could not connect to the proxy server.")
            return None
        except requests.exceptions.SSLError as e:
            logger.error(f"❌ SSL Error: {e}")
            return None
        except requests.RequestException as e:
            logger.error(f"❌ Connection error: {e}")
            return None

    @staticmethod
    def extract_tax_records(html: str) -> List[TaxRecord]:
        soup = BeautifulSoup(html, "lxml")

        table = soup.find("table", class_="ta_border")
        if not table:
            tables = soup.find_all("table")
            if tables:
                table = tables[0]
            else:
                logger.warning("❌ Data table not found in HTML.")
                return []

        rows = table.select("tr")
        if rows and rows[0].find("th"):
            rows = rows[1:]

        parsed_items: List[TaxRecord] = []

        for tr in rows:
            cols = tr.find_all("td")
            if len(cols) < 4:
                continue

            raw_name = cols[1].get_text(strip=True)

            tax_match = TAX_ID_PATTERN.search(raw_name)
            if not tax_match:
                continue

            mst = tax_match.group(1)
            name = CLEAN_NAME_PATTERN.sub("", raw_name)

            link_tag = cols[3].find("a")
            link_href = link_tag.get("href", "").strip() if link_tag else cols[3].get_text(strip=True)

            item: TaxRecord = {
                "stt": cols[0].get_text(strip=True),
                "ten_to_chuc": name,
                "mst": mst,
                "dia_chi": cols[2].get_text(strip=True),
                "trang_thong_tin": link_href
            }
            parsed_items.append(item)

        return parsed_items

    @staticmethod
    def extract_source_date(html: str) -> Optional[str]:
        soup = BeautifulSoup(html, "lxml")

        p_tag = soup.find("p", attrs={"dir": "ltr"})
        if not p_tag:
            return None

        text = p_tag.get_text(strip=True)
        match = DATE_PATTERN.search(text)

        if match:
            return f"{match.group(1)}/{match.group(2)}"

        return None


# =======================
#      FILE UTILS
# =======================

class DataManager:
    @staticmethod
    def load_database(path: str) -> Dict[str, TaxRecord]:
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return {item["mst"]: item for item in data if "mst" in item}
        except Exception as e:
            logger.error(f"⚠️ Error loading database: {e}")
            return {}

    @staticmethod
    def save_database(data: List[TaxRecord], path: str):
        def sort_key(x):
            try:
                return int(x.get("stt", 999999))
            except ValueError:
                return 999999

        data.sort(key=sort_key)

        tmp_path = path + ".tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

            shutil.move(tmp_path, path)
            logger.info(f"💾 Database saved with {len(data)} records to {path}")
        except Exception as e:
            logger.error(f"❌ Error saving database: {e}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    @staticmethod
    def upsert_records(existing: Dict[str, TaxRecord], new_items: List[TaxRecord]) -> List[TaxRecord]:
        new_count = 0
        update_count = 0
        unchanged_count = 0

        for item in new_items:
            mst = item["mst"]
            if mst not in existing:
                existing[mst] = item
                new_count += 1
            else:
                if existing[mst] != item:
                    existing[mst].update(item)
                    update_count += 1
                else:
                    unchanged_count += 1

        logger.info(f"📊 Sync Stats: New={new_count} | Updated={update_count} | Unchanged={unchanged_count}")
        return list(existing.values())

    @staticmethod
    def get_last_sync_date(path: str) -> Optional[str]:
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                date_str = f.read().strip()
            return date_str if date_str else None
        except Exception:
            return None

    @staticmethod
    def update_sync_date(date_str: Optional[str], path: str):
        if not date_str:
            return
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(date_str.strip())
            logger.info(f"📅 Sync date updated: {date_str}")
        except IOError as e:
            logger.error(f"❌ Failed to update sync date: {e}")


# =======================
#      MAIN EXECUTION
# =======================

def main():
    scraper = GDTScraper()

    # 1. Fetch Source
    html = scraper.fetch_html(TARGET_URL)
    if not html:
        return

    # 2. Check Source Date
    source_date = scraper.extract_source_date(html)
    if not source_date:
        logger.warning("⚠️ Could not determine source date. Aborting safety check.")
        return

    last_sync_date = DataManager.get_last_sync_date(DATE_FILE)

    # Optimization: Stop if dates match
    if last_sync_date == source_date:
        logger.info(f"✅ Database is up-to-date ({source_date}). No action needed.")
        return

    # 3. Parse Data
    new_records = scraper.extract_tax_records(html)
    logger.info(f"🔎 Extracted {len(new_records)} records from source.")

    if not new_records:
        logger.warning("No records found in the table.")
        return

    # 4. Process & Save
    current_db = DataManager.load_database(HDDTVN_FILE)
    merged_list = DataManager.upsert_records(current_db, new_records)

    DataManager.save_database(merged_list, HDDTVN_FILE)
    DataManager.update_sync_date(source_date, DATE_FILE)


if __name__ == "__main__":
    main()
