import os
import json
import time
import csv
import random
import signal
import re
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

ASSETS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "assets"))
INPUT_FILE = os.path.join(ASSETS_DIR, "pmc_data.csv")
RESULTS_FILE = os.path.join(ASSETS_DIR, "pmc_details.csv")
STATE_FILE = os.path.join(ASSETS_DIR, "pmc_details_crawl_state.json")
CHROME_PROFILE_PATH = os.path.join(os.getcwd(), "..", "tmp","pmc_profile")

class DetailCrawler:
    def __init__(self):
        self.running = True
        self.processed_ids = set()
        
        if not os.path.exists(ASSETS_DIR):
            os.makedirs(ASSETS_DIR)

        self.state = self.load_state()
        self.load_processed_ids()
        
        signal.signal(signal.SIGINT, self.handle_exit)

    def handle_exit(self, signum, frame):
        print("\n[!] Exit signal received. Saving state...")
        self.running = False

    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except: pass
        return {"last_processed_index": 0}

    def save_state(self, index):
        with open(STATE_FILE, 'w') as f:
            json.dump({"last_processed_index": index}, f)

    def load_processed_ids(self):
        if os.path.exists(RESULTS_FILE):
            with open(RESULTS_FILE, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    self.processed_ids.add(row['id'])
            print(f"Resuming: {len(self.processed_ids)} records already detailed.")

    def clean_one_line(self, text):
        if not text: return ""
        text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        return re.sub(r'\s+', ' ', text).strip()

    def get_text(self, parent, selector, default=""):
        try:
            return parent.find_element(By.CSS_SELECTOR, selector).text.strip()
        except:
            return default

    def extract_deep_data(self, driver, project_id):
        try:
            category_els = driver.find_elements(By.CSS_SELECTOR, ".post_context a")
            map_category = category_els[1].text.strip() if len(category_els) > 1 else "Unknown"
        except:
            map_category = "Unknown"

        author_id, author_level, author_rank, author_subs = "Unknown", "0", "", "0"
        try:
            mini_profile = driver.find_element(By.CSS_SELECTOR, ".mini-profile")
            author_id = mini_profile.find_element(By.ID, "author_id").get_attribute("innerText").strip()
            level_text = mini_profile.find_element(By.CSS_SELECTOR, ".mini-info").text
            lvl_match = re.search(r"Level\s+(\d+)", level_text)
            if lvl_match: author_level = lvl_match.group(1)
            if ":" in level_text: author_rank = level_text.split(":")[-1].strip()
            author_subs = mini_profile.find_element(By.CSS_SELECTOR, ".num_subscribers").text.strip()
        except: pass

        tags_list = []
        try:
            tag_elements = driver.find_elements(By.CSS_SELECTOR, "#item_tags .tag a")
            tags_list = [t.text.strip() for t in tag_elements if t.text.strip()]
        except: pass

        desc_raw = self.get_text(driver, "#r-text-block")
        description = self.clean_one_line(desc_raw)

        images = []
        img_elements = driver.find_elements(By.CSS_SELECTOR, "#light-gallery a.rsImg")
        for img in img_elements:
            href = img.get_attribute("href")
            if href: images.append(href)

        published_date, updated_date = "", ""
        try:
            date_block = driver.find_element(By.CSS_SELECTOR, ".post_date")
            dates = date_block.find_elements(By.TAG_NAME, "abbr")
            if len(dates) >= 1: updated_date = dates[0].get_attribute("title")
            if len(dates) >= 2: published_date = dates[1].get_attribute("title")
        except: pass

        mirrors = []
        try:
            # Targeting the specific container you identified
            resource_options = driver.find_element(By.ID, "resource-options")
            mirror_elements = resource_options.find_elements(By.CSS_SELECTOR, ".content-actions li a")
            
            for m in mirror_elements:
                m_classes = m.get_attribute("class") or ""
                m_title = m.get_attribute("title") or ""
                m_name = self.clean_one_line(m.text)
                
                # Check if it's a third-party link (where the real URL is in the title)
                if "third-party-download" in m_classes:
                    # Regex to find a URL starting with http/https inside the title string
                    url_match = re.search(r'https?://[^\s]+', m_title)
                    if url_match:
                        actual_url = url_match.group(0)
                        mirrors.append(f"{m_name} ({actual_url})")
                else:
                    # For standard branded-downloads, use the href
                    m_url = m.get_attribute("href")
                    if m_url:
                        # Convert relative URLs to absolute if necessary
                        if m_url.startswith('/'):
                            m_url = "https://www.planetminecraft.com" + m_url
                        mirrors.append(f"{m_name} ({m_url})")
        except Exception as e:
            print(f"Error extracting mirrors: {e}")

        # ... [Rest of the data dict remains the same] ...
        return {
            "id": project_id,
            "author_id": author_id,
            "author_level": author_level,
            "author_rank": author_rank,
            "author_subs": author_subs,
            "category": map_category,
            "tags": " | ".join(tags_list),
            "download_mirrors": " | ".join(mirrors), # This will now contain the cleaned URLs
            "description": description,
            "date_published": published_date,
            "date_updated": updated_date,
            "gallery_urls": " | ".join(images)
        }

    def run(self):
        if not os.path.exists(INPUT_FILE):
            print(f"Error: {INPUT_FILE} not found.")
            return

        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            items = list(csv.DictReader(f))

        i = self.state["last_processed_index"]
        driver = None

        while i < len(items) and self.running:
            try:
                if not driver:
                    options = uc.ChromeOptions()
                    options.page_load_strategy = 'eager'
                    prefs = {"profile.managed_default_content_settings.images": 2}
                    options.add_experimental_option("prefs", prefs)
                    print(f"--- Starting new Chrome instance at index {i} ---")
                    driver = uc.Chrome(options=options, version_main=144)
                    driver.set_page_load_timeout(120)

                project = items[i]
                p_id = project['id']
                
                if p_id in self.processed_ids:
                    i += 1
                    continue

                print(f"[{i+1}/{len(items)}] Scraping: {project['title']}...")
                
                driver.get(project['url'])

                is_404 = len(driver.find_elements(By.XPATH, "//h1[text()='404 Not Found']")) > 0
                
                if is_404:
                    print(f"--- [!] 404 detected for {project['title']}. Removing from CSV. ---")
                    items.pop(i) 
                    
                    with open(INPUT_FILE, 'w', newline='', encoding='utf-8') as f:
                        if items:
                            writer = csv.DictWriter(f, fieldnames=items[0].keys())
                            writer.writeheader()
                            writer.writerows(items)
                    
                    self.save_state(i)
                    continue 

                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "resource_object")))
                time.sleep(random.uniform(3.0, 10.0))

                deep_data = self.extract_deep_data(driver, p_id)
                
                file_exists = os.path.isfile(RESULTS_FILE)
                with open(RESULTS_FILE, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=deep_data.keys())
                    if not file_exists: writer.writeheader()
                    writer.writerow(deep_data)

                self.processed_ids.add(p_id)
                self.save_state(i)
                i += 1

            except Exception as e:
                print(f"\n[!] ERROR at index {i}: {e}")
                if driver:
                    try: driver.quit()
                    except: pass
                    driver = None 
                
                print("Retrying same item in 10 seconds...")
                time.sleep(10)

        if driver:
            driver.quit()

if __name__ == "__main__":
    crawler = DetailCrawler()
    crawler.run()