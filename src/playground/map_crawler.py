import os
import sys
import time
import random
import re
import zipfile
import shutil
import pandas as pd
import requests
import rarfile
import py7zr
from abc import ABC, abstractmethod
from urllib.parse import urlparse, parse_qs
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Configuration ---
ASSETS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "assets"))
TMP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "tmp"))
DOWNLOADS_DIR = TMP_DIR
EXTRACT_DIR = os.path.join(TMP_DIR, "downloads")
RESULTS_FILE = os.path.join(ASSETS_DIR, "map_crawl_results.csv")
INPUT_FILE = os.path.join(ASSETS_DIR, "pmc_data_cleansed.csv")

for d in [ASSETS_DIR, DOWNLOADS_DIR, EXTRACT_DIR]:
    os.makedirs(d, exist_ok=True)

def log(msg, level="INFO"):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")

# --- Strategy Pattern for URL Resolvers ---

class ResolutionStrategy(ABC):
    @abstractmethod
    def resolve(self, url, driver):
        pass

class MediaFireResolver(ResolutionStrategy):
    def resolve(self, url, driver):
        original_window = driver.current_window_handle
        driver.switch_to.new_window('tab')
        try:
            driver.get(url)
            btn = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "downloadButton"))
            )
            return btn.get_attribute('href')
        except Exception as e:
            log(f"MediaFire Error: {str(e)}", "DEBUG")
            return None
        finally:
            driver.close()
            driver.switch_to.window(original_window)

class DropboxResolver(ResolutionStrategy):
    def resolve(self, url, driver):
        return url.replace("dl=0", "dl=1")

class GoogleDriveResolver(ResolutionStrategy):
    def resolve(self, url, driver):
        parsed = urlparse(url)
        file_id = None
        if "/file/d/" in parsed.path:
            file_id = parsed.path.split("/file/d/")[1].split("/")[0]
        elif "id" in parse_qs(parsed.query):
            file_id = parse_qs(parsed.query)['id'][0]
        return f"https://drive.google.com/uc?export=download&id={file_id}" if file_id else url

class URLResolverContext:
    def __init__(self, driver):
        self.driver = driver
        self.strategies = {
            "mediafire.com": MediaFireResolver(),
            "dropbox.com": DropboxResolver(),
            "drive.google.com": GoogleDriveResolver()
        }

    def resolve(self, url):
        for domain, strategy in self.strategies.items():
            if domain in url:
                log(f"Using {domain} strategy for {url}")
                return strategy.resolve(url, self.driver)
        return url

# --- Logic for File Handling ---

class MapProcessor:
    @staticmethod
    def process(file_path, project_id):
        log(f"Processing ID {project_id}: Inspecting archive {os.path.basename(file_path)}")
        try:
            archive = None
            fmt = None
            namelist = None

            if zipfile.is_zipfile(file_path):
                archive = zipfile.ZipFile(file_path, 'r')
                fmt = "ZIP"
                namelist = archive.namelist()

            elif rarfile.is_rarfile(file_path):
                archive = rarfile.RarFile(file_path, 'r')
                fmt = "RAR"
                namelist = archive.namelist()

            elif py7zr.is_7zfile(file_path):
                archive = py7zr.SevenZipFile(file_path, 'r')
                fmt = "7Z"
                namelist = archive.getnames()

            else:
                return False, "Invalid/Incomplete Archive (Not Zip/Rar/7z)"

            with archive:
                dat_path = next((f for f in namelist if f.endswith('level.dat')), None)
                if not dat_path:
                    return False, "level.dat missing recursively"

                root_prefix = os.path.dirname(dat_path)
                if root_prefix:
                    root_prefix += '/'

                target_dir = os.path.join(EXTRACT_DIR, str(project_id))
                if os.path.exists(target_dir):
                    shutil.rmtree(target_dir)
                os.makedirs(target_dir, exist_ok=True)

                count = 0

                for member in namelist:
                    if not member.startswith(root_prefix):
                        continue

                    rel_path = os.path.relpath(member, root_prefix)
                    if rel_path == ".":
                        continue

                    target_path = os.path.join(target_dir, rel_path)

                    if member.endswith('/'):
                        os.makedirs(target_path, exist_ok=True)
                    else:
                        os.makedirs(os.path.dirname(target_path), exist_ok=True)

                        if fmt == "7Z":
                            archive.extract(targets=[member], path=target_dir)
                        else:
                            with archive.open(member) as source, open(target_path, "wb") as target:
                                shutil.copyfileobj(source, target)

                        count += 1

                return True, f"Success: {fmt} ({count} files) from {dat_path}"

        except Exception as e:
            return False, f"Extraction Error: {str(e)}"


# --- Main Manager ---

class MapArtifactManager:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "Mozilla/5.0"})
        self.columns = ['project_id', 'success', 'status_msg', 'resolved_url', 'timestamp']

    def _get_attempted_ids(self):
        """Loads IDs that are already in the results CSV to skip them."""
        if not os.path.exists(RESULTS_FILE):
            return set()
        try:
            df = pd.read_csv(RESULTS_FILE)
            return set(df['project_id'].astype(str).unique())
        except Exception as e:
            log(f"Error reading state: {e}", "ERROR")
            return set()

    def _download_file(self, url, dest):
        try:
            log(f"Downloading stream from {url[:50]}...")
            with self.session.get(url, stream=True, timeout=60) as r:
                # Explicit 404 Check
                if r.status_code == 404:
                    return False, "404 Not Found (Link Dead)"
                
                # Handle other 4xx/5xx errors
                r.raise_for_status() 
                
                total_size = int(r.headers.get('content-length', 0))
                downloaded = 0
                
                with open(dest, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=512*1024):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                percent = int(100 * downloaded / total_size)
                                sys.stdout.write(f"\r    [Progress: {percent}%] {downloaded//1024}KB")
                                sys.stdout.flush()
                print() 
                
                if total_size > 0 and downloaded < total_size:
                    return False, f"Incomplete: {downloaded}/{total_size} bytes"
                return True, None
        except requests.exceptions.HTTPError as e:
            return False, f"HTTP Error: {e.response.status_code}"
        except Exception as e:
            return False, str(e)

    def _handle_item(self, p_id, mirrors, resolver):
        # Find ALL URLs inside parentheses
        urls = re.findall(r'\((https?://[^)]+)\)', mirrors)
        
        if not urls:
            return False, "No URL in mirror string", None
        
        last_error = "All mirrors failed"
        
        # Iterate through every URL found for this project
        for raw_url in urls:
            final_url = None
            try:
                log(f"Attempting mirror: {raw_url}")
                final_url = resolver.resolve(raw_url)
                if not final_url:
                    last_error = f"Resolution failed for {raw_url}"
                    continue # Try the next URL
                
                temp_file = os.path.join(DOWNLOADS_DIR, f"{p_id}_tmp")
                success, err = self._download_file(final_url, temp_file)
                
                if not success:
                    if os.path.exists(temp_file): os.remove(temp_file)
                    last_error = f"Download Error: {err}"
                    continue # Try the next URL
                
                ok, msg = MapProcessor.process(temp_file, p_id)
                if os.path.exists(temp_file): os.remove(temp_file)
                
                if ok:
                    return True, msg, final_url # Success! Exit the loop
                else:
                    last_error = msg # Extraction failed, try next mirror
                    
            except Exception as e:
                last_error = f"Internal Error: {str(e)}"
                continue

        # If we get here, all mirrors were exhausted
        return False, last_error, urls[0] if urls else None

    def _init_driver(self):
        log("Initializing fresh Chrome driver...")
        options = uc.ChromeOptions()
        # Add headless if you don't need to see the window
        # options.add_argument('--headless') 
        return uc.Chrome(options=options, version_main=144)

    def run(self):
        log("Checking for pending maps...")
        df_input = pd.read_csv(INPUT_FILE)
        
        # Outer loop to handle driver restarts
        while True:
            attempted_ids = self._get_attempted_ids()
            pending_df = df_input[~df_input['id'].astype(str).isin(attempted_ids)]

            if pending_df.empty:
                log("All maps in input have been attempted.")
                break

            log(f"Starting batch of {len(pending_df)} maps.")
            driver = self._init_driver()
            resolver = URLResolverContext(driver)

            try:
                for _, row in pending_df.iterrows():
                    p_id = str(row['id'])
                    mirrors = str(row.get('download_mirrors', ''))
                    
                    log(f"--- ID: {p_id} ---")
                    
                    try:
                        success, status_msg, resolved_url = self._handle_item(p_id, mirrors, resolver)
                    except Exception as e:
                        # Check if the error is a connection failure to the driver
                        if "Connection refused" in str(e) or "Max retries exceeded" in str(e):
                            log("Detected Chrome crash/disconnection. Restarting driver...", "ERROR")
                            break # Break the inner for-loop to trigger a driver restart
                        else:
                            success, status_msg, resolved_url = False, f"Fatal Item Error: {str(e)}", None

                    # --- SANITIZATION & SAVING ---
                    clean_msg = str(status_msg).replace('\n', ' ').replace('\r', ' ').strip()
                    res_df = pd.DataFrame([{
                        'project_id': p_id,
                        'success': 'Yes' if success else 'No',
                        'status_msg': clean_msg,
                        'resolved_url': resolved_url,
                        'timestamp': time.strftime("%Y-%m-%d %H:%M:%S")
                    }])
                    
                    res_df.to_csv(
                        RESULTS_FILE, 
                        mode='a', 
                        index=False, 
                        header=not os.path.exists(RESULTS_FILE),
                        lineterminator='\n',
                    )
                    
                    log(f"Result for {p_id}: {'[+]' if success else '[-]'} {clean_msg}")
                    time.sleep(random.uniform(2.0, 4.0))

            except Exception as global_e:
                log(f"Unexpected loop error: {global_e}", "CRITICAL")
            finally:
                log("Cleaning up driver...")
                try:
                    driver.quit()
                except:
                    pass # Driver might already be dead

if __name__ == "__main__":
    MapArtifactManager().run()