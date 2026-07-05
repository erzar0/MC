"""Planet Minecraft per-project detail crawler.

Visits each project URL from the listing CSV and appends deep metadata
(author info, tags, description, download mirrors, gallery) to the details
CSV. Resumable: the row index cursor and 404'd project IDs are persisted to
a state JSON; already-detailed IDs are deduped from the output CSV.
"""

import argparse
import csv
import json
import logging
import random
import re
import signal
import time
from pathlib import Path

import undetected_chromedriver as uc
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("pmc_detail_crawler")

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ASSETS_DIR = PROJECT_ROOT / "assets"
DEFAULT_INPUT_FILE = ASSETS_DIR / "pmc_data.csv"
DEFAULT_RESULTS_FILE = ASSETS_DIR / "pmc_details.csv"
DEFAULT_STATE_FILE = ASSETS_DIR / "pmc_details_crawl_state.json"

PMC_BASE_URL = "https://www.planetminecraft.com"


class DetailCrawler:
    """Resumable per-project detail crawler."""

    def __init__(
        self,
        input_file: Path = DEFAULT_INPUT_FILE,
        results_file: Path = DEFAULT_RESULTS_FILE,
        state_file: Path = DEFAULT_STATE_FILE,
        chrome_version: int | None = None,
    ):
        self.input_file = Path(input_file)
        self.results_file = Path(results_file)
        self.state_file = Path(state_file)
        self.chrome_version = chrome_version

        self.running = True
        self.processed_ids: set[str] = set()

        ASSETS_DIR.mkdir(parents=True, exist_ok=True)
        self.state = self._load_state()
        self._load_processed_ids()

        signal.signal(signal.SIGINT, self._handle_exit)

    def _handle_exit(self, signum, frame):
        log.info("Exit signal received. Saving state...")
        self.running = False

    # ------------------------------------------------------------------
    # State handling
    # ------------------------------------------------------------------

    def _load_state(self) -> dict:
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                state.setdefault("skipped_404_ids", [])
                return state
            except (OSError, json.JSONDecodeError) as e:
                log.warning(f"Failed to load state file {self.state_file}: {e}. Starting fresh.")
        return {"last_processed_index": 0, "skipped_404_ids": []}

    def _save_state(self, index: int) -> None:
        self.state["last_processed_index"] = index
        with open(self.state_file, "w") as f:
            json.dump(self.state, f)

    def _load_processed_ids(self) -> None:
        if self.results_file.exists():
            with open(self.results_file, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    self.processed_ids.add(row["id"])
            log.info(f"Resuming: {len(self.processed_ids)} records already detailed.")

    # ------------------------------------------------------------------
    # Extraction helpers (one per page section)
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_one_line(text: str) -> str:
        if not text:
            return ""
        text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
        return re.sub(r"\s+", " ", text).strip()

    def _get_text(self, parent, selector: str, default: str = "") -> str:
        try:
            return parent.find_element(By.CSS_SELECTOR, selector).text.strip()
        except NoSuchElementException:
            return default

    def _extract_category(self, driver) -> str:
        category_els = driver.find_elements(By.CSS_SELECTOR, ".post_context a")
        return category_els[1].text.strip() if len(category_els) > 1 else "Unknown"

    def _extract_author(self, driver) -> dict:
        author = {"author_id": "Unknown", "author_level": "0", "author_rank": "", "author_subs": "0"}
        try:
            mini_profile = driver.find_element(By.CSS_SELECTOR, ".mini-profile")
            author["author_id"] = mini_profile.find_element(By.ID, "author_id").get_attribute("innerText").strip()
            level_text = mini_profile.find_element(By.CSS_SELECTOR, ".mini-info").text
            lvl_match = re.search(r"Level\s+(\d+)", level_text)
            if lvl_match:
                author["author_level"] = lvl_match.group(1)
            if ":" in level_text:
                author["author_rank"] = level_text.split(":")[-1].strip()
            author["author_subs"] = mini_profile.find_element(By.CSS_SELECTOR, ".num_subscribers").text.strip()
        except NoSuchElementException as e:
            log.debug(f"Author profile not fully available: {e}")
        return author

    def _extract_tags(self, driver) -> list[str]:
        tag_elements = driver.find_elements(By.CSS_SELECTOR, "#item_tags .tag a")
        return [t.text.strip() for t in tag_elements if t.text.strip()]

    def _extract_gallery(self, driver) -> list[str]:
        img_elements = driver.find_elements(By.CSS_SELECTOR, "#light-gallery a.rsImg")
        return [img.get_attribute("href") for img in img_elements if img.get_attribute("href")]

    def _extract_dates(self, driver) -> tuple[str, str]:
        """Returns (published_date, updated_date)."""
        published_date, updated_date = "", ""
        try:
            date_block = driver.find_element(By.CSS_SELECTOR, ".post_date")
            dates = date_block.find_elements(By.TAG_NAME, "abbr")
            if len(dates) >= 1:
                updated_date = dates[0].get_attribute("title")
            if len(dates) >= 2:
                published_date = dates[1].get_attribute("title")
        except NoSuchElementException:
            pass
        return published_date, updated_date

    def _extract_mirrors(self, driver) -> list[str]:
        """Collects download mirrors as 'Name (url)' strings."""
        mirrors = []
        try:
            resource_options = driver.find_element(By.ID, "resource-options")
            mirror_elements = resource_options.find_elements(By.CSS_SELECTOR, ".content-actions li a")

            for m in mirror_elements:
                m_classes = m.get_attribute("class") or ""
                m_title = m.get_attribute("title") or ""
                m_name = self._clean_one_line(m.text)

                # Third-party links hide the real URL in the title attribute
                if "third-party-download" in m_classes:
                    url_match = re.search(r"https?://\S+", m_title)
                    if url_match:
                        mirrors.append(f"{m_name} ({url_match.group(0)})")
                else:
                    # For standard branded-downloads, use the href
                    m_url = m.get_attribute("href")
                    if m_url:
                        if m_url.startswith("/"):
                            m_url = PMC_BASE_URL + m_url
                        mirrors.append(f"{m_name} ({m_url})")
        except NoSuchElementException as e:
            log.warning(f"Error extracting mirrors: {e}")
        return mirrors

    def extract_deep_data(self, driver, project_id: str) -> dict:
        """Extracts all detail fields for the currently loaded project page."""
        try:
            map_category = self._extract_category(driver)
        except NoSuchElementException:
            map_category = "Unknown"

        author = self._extract_author(driver)
        published_date, updated_date = self._extract_dates(driver)

        return {
            "id": project_id,
            **author,
            "category": map_category,
            "tags": " | ".join(self._extract_tags(driver)),
            "download_mirrors": " | ".join(self._extract_mirrors(driver)),
            "description": self._clean_one_line(self._get_text(driver, "#r-text-block")),
            "date_published": published_date,
            "date_updated": updated_date,
            "gallery_urls": " | ".join(self._extract_gallery(driver)),
        }

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _build_driver(self, index: int) -> uc.Chrome:
        options = uc.ChromeOptions()
        options.page_load_strategy = "eager"
        options.add_experimental_option("prefs", {"profile.managed_default_content_settings.images": 2})
        log.info(f"--- Starting new Chrome instance at index {index} ---")
        driver = uc.Chrome(options=options, version_main=self.chrome_version)
        driver.set_page_load_timeout(120)
        return driver

    def _append_result(self, deep_data: dict) -> None:
        file_exists = self.results_file.is_file()
        with open(self.results_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=deep_data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(deep_data)

    def run(self) -> None:
        if not self.input_file.exists():
            log.error(f"Input file not found: {self.input_file}")
            return

        with open(self.input_file, "r", encoding="utf-8") as f:
            items = list(csv.DictReader(f))

        skipped_404 = set(self.state.get("skipped_404_ids", []))
        i = self.state["last_processed_index"]
        driver = None

        while i < len(items) and self.running:
            try:
                if not driver:
                    driver = self._build_driver(i)

                project = items[i]
                p_id = project["id"]

                if p_id in self.processed_ids or p_id in skipped_404:
                    i += 1
                    continue

                log.info(f"[{i + 1}/{len(items)}] Scraping: {project['title']}...")

                driver.get(project["url"])

                is_404 = len(driver.find_elements(By.XPATH, "//h1[text()='404 Not Found']")) > 0
                if is_404:
                    # The input CSV is left untouched; 404s are recorded in the
                    # state file and skipped on future runs.
                    log.warning(f"404 detected for {project['title']}. Recording and skipping.")
                    skipped_404.add(p_id)
                    self.state["skipped_404_ids"] = sorted(skipped_404)
                    i += 1
                    self._save_state(i)
                    continue

                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "resource_object")))
                time.sleep(random.uniform(3.0, 10.0))

                deep_data = self.extract_deep_data(driver, p_id)
                self._append_result(deep_data)

                self.processed_ids.add(p_id)
                self._save_state(i)
                i += 1

            except Exception as e:
                log.error(f"ERROR at index {i}: {e}")
                if driver:
                    try:
                        driver.quit()
                    except Exception as quit_err:
                        log.debug(f"Error quitting driver: {quit_err}")
                    driver = None

                log.info("Retrying same item in 10 seconds...")
                time.sleep(10)

        if driver:
            driver.quit()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl per-project details from Planet Minecraft")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_FILE, help="Listing CSV from pmc_data_crawler")
    parser.add_argument("--output", type=Path, default=DEFAULT_RESULTS_FILE, help="Output details CSV")
    parser.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE, help="Resumable cursor state JSON")
    parser.add_argument(
        "--chrome-version", type=int, default=None, help="Pin undetected-chromedriver version_main (default: auto)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    crawler = DetailCrawler(
        input_file=args.input,
        results_file=args.output,
        state_file=args.state_file,
        chrome_version=args.chrome_version,
    )
    crawler.run()
