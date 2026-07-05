"""Planet Minecraft listing crawler.

Walks the PMC projects listing (filtered by platform/year) with
undetected-chromedriver and appends one CSV row per project to the results
file. Resumable: the (year, page) cursor is persisted to a small state JSON
and already-seen URLs are deduped from the existing CSV.
"""

import argparse
import csv
import json
import logging
import random
import signal
import time
from pathlib import Path

import undetected_chromedriver as uc
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("pmc_data_crawler")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ASSETS_DIR = PROJECT_ROOT / "assets"
DEFAULT_STATE_FILE = ASSETS_DIR / "crawl_state.json"
DEFAULT_RESULTS_FILE = ASSETS_DIR / "pmc_data_crawl_state.csv"
DEFAULT_CHROME_PROFILE = PROJECT_ROOT / "tmp" / "prof"

# PMC platform filter values
PLATFORM_JAVA = 1
PLATFORM_BEDROCK = 2

LISTING_URL_TEMPLATE = (
    "https://www.planetminecraft.com/projects/?mode=advanced&share%5B%5D=world_link"
    "&platform={platform}&monetization%5B%5D=0&monetization%5B%5D=1"
    "&time_machine=y-{year}&order=order_downloads&p={page}"
)


class Crawler:
    """Resumable PMC listing crawler."""

    def __init__(
        self,
        platform: int = PLATFORM_BEDROCK,
        years: list[int] | None = None,
        state_file: Path = DEFAULT_STATE_FILE,
        results_file: Path = DEFAULT_RESULTS_FILE,
        chrome_profile: Path = DEFAULT_CHROME_PROFILE,
        chrome_version: int | None = None,
    ):
        self.platform = platform
        self.years = years or list(range(2015, 2027))
        self.state_file = Path(state_file)
        self.results_file = Path(results_file)
        self.chrome_profile = Path(chrome_profile)
        self.chrome_version = chrome_version

        self.running = True
        self.processed_urls: set[str] = set()

        ASSETS_DIR.mkdir(parents=True, exist_ok=True)
        self.state = self._load_initial_state()
        self._load_processed_from_csv()

        signal.signal(signal.SIGINT, self._handle_exit)

    def _handle_exit(self, signum, frame):
        log.info("Exit signal received. Saving progress and closing browser...")
        self.running = False

    # ------------------------------------------------------------------
    # State handling
    # ------------------------------------------------------------------

    def _load_initial_state(self) -> dict:
        """Loads the (year_idx, page) cursor, starting fresh on any error."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    return json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                log.warning(f"Failed to load state file {self.state_file}: {e}. Starting fresh.")
        return {"year_idx": 0, "page": 1}

    def _load_processed_from_csv(self) -> None:
        if self.results_file.exists():
            log.info(f"Loading previous results from {self.results_file}...")
            with open(self.results_file, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    self.processed_urls.add(row["url"])
            log.info(f"Deduped {len(self.processed_urls)} records.")

    def _save_progress(self, y_idx: int, p_num: int) -> None:
        with open(self.state_file, "w") as f:
            json.dump({"year_idx": y_idx, "page": p_num}, f)

    # ------------------------------------------------------------------
    # Scraping helpers
    # ------------------------------------------------------------------

    def _get_safe_text(self, parent, selector: str, attr: str | None = None) -> str:
        try:
            el = parent.find_element(By.CSS_SELECTOR, selector)
            return el.get_attribute(attr) if attr else el.text.strip()
        except NoSuchElementException:
            return "0" if "num" in selector or "span" in selector else ""

    def _build_driver(self) -> uc.Chrome:
        log.info("Starting/Restarting browser instance...")
        options = uc.ChromeOptions()
        options.add_argument(f"--user-data-dir={self.chrome_profile}")
        # A pinned version_main can break when Chrome updates; pass None to
        # let undetected-chromedriver auto-detect.
        return uc.Chrome(options=options, version_main=self.chrome_version)

    def _extract_item(self, item, year: int) -> dict | None:
        """Extracts one listing row; returns None for already-seen URLs."""
        title_el = item.find_element(By.CSS_SELECTOR, "a.r-title")
        href = title_el.get_attribute("href")

        if href in self.processed_urls:
            return None

        return {
            "id": item.get_attribute("data-id"),
            "year_filter": year,
            "title": title_el.text.strip(),
            "url": href,
            "category": self._get_safe_text(item, ".r-subject"),
            "creator": self._get_safe_text(item, ".activity_name"),
            "creator_id": self._get_safe_text(item, ".activity_name", "data-mid"),
            "views": self._get_safe_text(item, "i.visibility + span"),
            "downloads": self._get_safe_text(item, "i.get_app + span"),
            "comments": self._get_safe_text(item, "i.chat_bubble + span"),
            "diamonds": self._get_safe_text(item, ".c-num-votes"),
            "favorites": self._get_safe_text(item, ".c-num-favs"),
            "published_date": self._get_safe_text(item, ".contributed abbr.timeago", "title"),
            "scraped_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "platform": "bedrock" if self.platform == PLATFORM_BEDROCK else "java",
        }

    def _append_result(self, data: dict) -> None:
        file_exists = self.results_file.is_file()
        with open(self.results_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)

    def _scrape_items(self, items, year: int) -> None:
        for item in items:
            if not self.running:
                break
            try:
                data = self._extract_item(item, year)
            except (NoSuchElementException, WebDriverException) as e:
                log.debug(f"Skipping listing item: {e}")
                continue
            if data is None:
                continue
            self._append_result(data)
            self.processed_urls.add(data["url"])

    def _has_next_page(self, driver) -> bool:
        try:
            driver.find_element(By.CSS_SELECTOR, "a.pagination_next")
            return True
        except NoSuchElementException:
            return False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        y_idx = self.state["year_idx"]
        p_num = self.state["page"]

        while y_idx < len(self.years) and self.running:
            driver = None
            try:
                driver = self._build_driver()

                # Internal loop for the actual scraping
                while y_idx < len(self.years) and self.running:
                    year = self.years[y_idx]
                    url = LISTING_URL_TEMPLATE.format(platform=self.platform, year=year, page=p_num)

                    log.info(f"--- [Year: {year}] [Page: {p_num}] [Unique: {len(self.processed_urls)}] ---")

                    # This is where the ReadTimeout usually happens
                    driver.get(url)

                    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    time.sleep(random.uniform(2.5, 4.0))

                    items = driver.find_elements(By.CSS_SELECTOR, "li.resource[data-type='resource']")

                    if not items:
                        log.info(f"Year {year} appears exhausted.")
                        y_idx += 1
                        p_num = 1
                        self._save_progress(y_idx, p_num)
                        continue

                    self._scrape_items(items, year)

                    if self._has_next_page(driver):
                        p_num += 1
                        self._save_progress(y_idx, p_num)
                        time.sleep(random.uniform(4.0, 7.0))
                    else:
                        log.info(f"End of Year {year}. Advancing...")
                        y_idx += 1
                        p_num = 1
                        self._save_progress(y_idx, p_num)

            except Exception as e:
                if not self.running:
                    break
                log.error(f"Driver crash or timeout: {e}. Restarting in 10s...")
                time.sleep(10)
            finally:
                if driver:
                    try:
                        driver.quit()
                    except Exception as e:
                        log.debug(f"Error quitting driver: {e}")

        log.info(f"[Done] Assets updated in: {ASSETS_DIR}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl Planet Minecraft project listings")
    parser.add_argument(
        "--platform",
        type=int,
        choices=[PLATFORM_JAVA, PLATFORM_BEDROCK],
        default=PLATFORM_BEDROCK,
        help="PMC platform filter (1=java, 2=bedrock)",
    )
    parser.add_argument(
        "--years", type=int, nargs="+", default=list(range(2015, 2027)), help="Years to scrape (time_machine filter)"
    )
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE, help="Resumable cursor state JSON")
    parser.add_argument("--output", type=Path, default=DEFAULT_RESULTS_FILE, help="Output CSV path")
    parser.add_argument("--chrome-profile", type=Path, default=DEFAULT_CHROME_PROFILE, help="Chrome user-data dir")
    parser.add_argument(
        "--chrome-version", type=int, default=None, help="Pin undetected-chromedriver version_main (default: auto)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    crawler = Crawler(
        platform=args.platform,
        years=args.years,
        state_file=args.state_file,
        results_file=args.output,
        chrome_profile=args.chrome_profile,
        chrome_version=args.chrome_version,
    )
    crawler.run()
