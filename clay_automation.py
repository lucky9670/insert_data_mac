"""
Clay.com Automation Script — Attach to Existing Chrome Session (FIXED)
========================================================================
This script attaches Selenium to your ALREADY OPEN Chrome browser where
you're logged into Clay.com. No login, no passwords, no profile copying.

FIXES:
    - Better Chrome process detection and cleanup
    - Improved debugging port handling
    - Added retry logic for navigation
    - Better session verification
    - Process cleanup on Windows

Requirements:
    pip install selenium webdriver-manager psutil

Usage:
    1. CLOSE Chrome completely (check Task Manager!)
    2. python clay_automation_fixed.py
"""

import os
import sys
import re
import time
import socket
import logging
import subprocess
import psutil
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
)

try:
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError:
    ChromeDriverManager = None

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLAY_BASE_URL = "https://app.clay.com"
DEFAULT_DOWNLOAD_DIR = str(Path.home() / "Downloads" / "clay_exports")
DEFAULT_TIMEOUT = 20  # seconds
INDUSTRY_FILTER_VALUE = "Technology"
TARGET_RECORD_COUNT = 5000
REMOTE_DEBUG_PORT = 9222

# Chrome paths — update CHROME_EXE if yours is different
CHROME_EXE = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
CHROME_PROFILE_DIR = r"C:\Users\hp\AppData\Local\Google\Chrome\User Data"
CHROME_PROFILE_NAME = "Profile 1"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("clay_automation")


# ---------------------------------------------------------------------------
# Helper Utilities
# ---------------------------------------------------------------------------

def wait_and_click(driver, locator, timeout=DEFAULT_TIMEOUT, description="element"):
    """Wait for an element to be clickable, then click it."""
    log.info(f"Waiting for {description}...")
    element = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable(locator)
    )
    try:
        element.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", element)
    log.info(f"Clicked {description}")
    return element


def wait_for_element(driver, locator, timeout=DEFAULT_TIMEOUT, description="element"):
    """Wait for an element to be present."""
    log.info(f"Waiting for {description}...")
    return WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located(locator)
    )


def is_port_in_use(port: int) -> bool:
    """Check if a TCP port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def kill_chrome_processes():
    """Kill all Chrome processes on Windows."""
    log.info("Checking for running Chrome processes...")
    killed = False
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if proc.info['name'] and 'chrome.exe' in proc.info['name'].lower():
                log.info(f"Killing Chrome process: PID {proc.info['pid']}")
                proc.kill()
                killed = True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    
    if killed:
        log.info("Waiting for Chrome to fully close...")
        time.sleep(3)
    else:
        log.info("No Chrome processes found running")
    
    return killed


# ---------------------------------------------------------------------------
# Main Automation Class
# ---------------------------------------------------------------------------

class ClayAutomation:
    """Automates Clay.com by attaching to your existing Chrome session."""

    def __init__(
        self,
        chrome_exe: str = CHROME_EXE,
        chrome_profile_dir: str = CHROME_PROFILE_DIR,
        chrome_profile_name: str = CHROME_PROFILE_NAME,
        debug_port: int = REMOTE_DEBUG_PORT,
        download_dir: str = DEFAULT_DOWNLOAD_DIR,
        industry_filter: str = INDUSTRY_FILTER_VALUE,
        target_records: int = TARGET_RECORD_COUNT,
        clay_workspace_url: str = None,
    ):
        self.chrome_exe = chrome_exe
        self.chrome_profile_dir = chrome_profile_dir
        self.chrome_profile_name = chrome_profile_name
        self.debug_port = debug_port
        self.download_dir = download_dir
        self.industry_filter = industry_filter
        self.target_records = target_records
        self.clay_workspace_url = clay_workspace_url
        self.driver = None
        self._chrome_process = None

        Path(self.download_dir).mkdir(parents=True, exist_ok=True)

    # ---- Browser Setup ----

    def _launch_chrome_with_debugging(self):
        """Launch Chrome with remote debugging using your real profile."""

        # First, kill any existing Chrome processes
        log.info("=" * 60)
        log.info("Step 0: Preparing Chrome for automation")
        log.info("=" * 60)
        
        if is_port_in_use(self.debug_port):
            log.warning(f"Port {self.debug_port} is already in use!")
            log.info("Attempting to kill existing Chrome processes...")
            kill_chrome_processes()
            
            # Wait and check again
            time.sleep(2)
            if is_port_in_use(self.debug_port):
                log.error(f"Port {self.debug_port} is still in use after cleanup!")
                log.error("Please manually close Chrome from Task Manager and try again.")
                raise RuntimeError("Debug port still in use after cleanup attempt")

        log.info("Launching Chrome with remote debugging enabled...")
        log.info(f"  Chrome: {self.chrome_exe}")
        log.info(f"  Profile: {self.chrome_profile_dir}")
        log.info(f"  Profile Name: {self.chrome_profile_name}")
        log.info(f"  Debug port: {self.debug_port}")

        cmd = [
            self.chrome_exe,
            f"--remote-debugging-port={self.debug_port}",
            f"--user-data-dir={self.chrome_profile_dir}",
            "--no-first-run",
            "--no-default-browser-check",
        ]

        try:
            # Use CREATE_NEW_PROCESS_GROUP on Windows to detach from parent
            creation_flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
            
            self._chrome_process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creation_flags,
            )
            log.info(f"Chrome process started with PID: {self._chrome_process.pid}")
        except FileNotFoundError:
            raise FileNotFoundError(
                f"Chrome not found at: {self.chrome_exe}\n"
                f"Please update CHROME_EXE in the script to your chrome.exe path."
            )

        # Wait for Chrome to start and open the debug port
        log.info("Waiting for Chrome to start and open debug port...")
        max_attempts = 60  # Increased to 60 seconds
        for i in range(max_attempts):
            if is_port_in_use(self.debug_port):
                log.info(f"✓ Chrome is ready! (debug port {self.debug_port} is open)")
                time.sleep(2)  # Give Chrome a bit more time to stabilize
                return
            time.sleep(1)
            if (i + 1) % 10 == 0:
                log.info(f"Still waiting... ({i + 1}/{max_attempts} seconds)")

        raise RuntimeError(
            f"Chrome did not open debug port {self.debug_port} within {max_attempts} seconds.\n"
            "Troubleshooting steps:\n"
            "1. Make sure Chrome is completely closed (check Task Manager)\n"
            "2. Check that no other program is using port 9222\n"
            "3. Try running the script as Administrator\n"
            "4. Verify Chrome executable path is correct"
        )

    def _attach_selenium(self) -> webdriver.Chrome:
        """Attach Selenium to the running Chrome instance via remote debugging."""
        log.info("Attaching Selenium to Chrome...")
        
        chrome_options = Options()
        chrome_options.add_experimental_option("debuggerAddress", f"127.0.0.1:{self.debug_port}")
        
        # Add preferences for downloads
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)

        try:
            if ChromeDriverManager:
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                driver = webdriver.Chrome(options=chrome_options)

            driver.implicitly_wait(5)
            log.info(f"✓ Selenium attached successfully!")
            log.info(f"Current URL: {driver.current_url}")
            return driver
        except Exception as e:
            log.error(f"Failed to attach Selenium: {e}")
            raise

    # ---- Step 1: Verify Clay Login ----

    def verify_clay_session(self):
        """Navigate to Clay and verify we're logged in."""
        log.info("=" * 60)
        log.info("STEP 1: Verifying Clay.com session")
        log.info("=" * 60)

        # If user provided a specific workspace URL, go there
        target_url = self.clay_workspace_url or CLAY_BASE_URL
        
        # First check current URL - if already on Clay, don't navigate
        current_url = self.driver.current_url.lower()
        
        if "clay.com" in current_url and "sign-in" not in current_url:
            log.info(f"Already on Clay! Current URL: {self.driver.current_url}")
        else:
            log.info(f"Navigating to: {target_url}")
            try:
                self.driver.get(target_url)
                log.info("Waiting for page to load...")
                time.sleep(8)  # Give page time to load
            except Exception as e:
                log.error(f"Navigation failed: {e}")
                return False

        # Refresh current URL after navigation
        current_url = self.driver.current_url.lower()
        log.info(f"Current URL after navigation: {current_url}")

        # Check if we landed on Clay (not a login/sign-in page)
        if "clay.com" in current_url and "sign-in" not in current_url and "login" not in current_url:
            log.info("✓ Successfully verified Clay session!")
            log.info("You appear to be logged in.")
            
            # Take a screenshot for verification
            self._take_screenshot("session_verified")
            return True
        else:
            log.error("✗ Not logged into Clay")
            log.error(f"Current URL: {self.driver.current_url}")
            log.error("Please log into Clay manually and run the script again.")
            self._take_screenshot("login_required")
            return False

    # ---- Step 2: Navigate to Table ----

    def navigate_to_table(self):
        """Navigate to the main data table in Clay."""
        log.info("=" * 60)
        log.info("STEP 2: Navigating to data table")
        log.info("=" * 60)

        # Multiple strategies to find and click "Tables" link/button
        table_selectors = [
            (By.XPATH, "//a[contains(text(), 'Tables')]"),
            (By.XPATH, "//button[contains(text(), 'Tables')]"),
            (By.XPATH, "//div[contains(text(), 'Tables')]"),
            (By.CSS_SELECTOR, "a[href*='tables']"),
            (By.CSS_SELECTOR, "[data-testid*='tables']"),
        ]

        clicked = False
        for selector in table_selectors:
            try:
                element = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable(selector)
                )
                element.click()
                log.info("✓ Clicked on Tables navigation")
                clicked = True
                break
            except (TimeoutException, NoSuchElementException):
                continue

        if not clicked:
            log.warning("Could not find 'Tables' navigation automatically")
            log.warning("Please manually navigate to your table in the browser")
            log.info("Waiting 15 seconds for manual navigation...")
            time.sleep(15)

        time.sleep(5)
        log.info(f"Current URL: {self.driver.current_url}")

    # ---- Step 3: Apply Industry Filter ----

    def filter_by_industry(self):
        """Apply industry filter to the data table."""
        log.info("=" * 60)
        log.info(f"STEP 3: Filtering by industry: {self.industry_filter}")
        log.info("=" * 60)

        try:
            # Look for filter button
            filter_selectors = [
                (By.XPATH, "//button[contains(., 'Filter')]"),
                (By.XPATH, "//button[contains(., 'Add filter')]"),
                (By.CSS_SELECTOR, "[data-testid*='filter']"),
                (By.XPATH, "//button[contains(@aria-label, 'filter')]"),
            ]

            for selector in filter_selectors:
                try:
                    wait_and_click(self.driver, selector, timeout=8, description="filter button")
                    break
                except TimeoutException:
                    continue
            else:
                raise NoSuchElementException("Filter button not found")

            time.sleep(2)

            # Look for Industry field
            industry_selectors = [
                (By.XPATH, "//input[@placeholder='Search columns...']"),
                (By.XPATH, "//input[contains(@placeholder, 'column')]"),
                (By.CSS_SELECTOR, "input[type='text']"),
            ]

            for selector in industry_selectors:
                try:
                    field = wait_for_element(self.driver, selector, timeout=5, description="column search")
                    field.click()
                    field.send_keys("Industry")
                    time.sleep(1)
                    field.send_keys(Keys.ENTER)
                    log.info("Selected 'Industry' column")
                    break
                except (TimeoutException, NoSuchElementException):
                    continue

            time.sleep(2)

            # Enter industry value
            value_selectors = [
                (By.XPATH, "//input[@placeholder='Enter value...']"),
                (By.XPATH, "//input[contains(@placeholder, 'value')]"),
                (By.CSS_SELECTOR, "input[type='text']"),
            ]

            for selector in value_selectors:
                try:
                    value_field = wait_for_element(self.driver, selector, timeout=5, description="value input")
                    value_field.click()
                    value_field.send_keys(self.industry_filter)
                    time.sleep(1)
                    value_field.send_keys(Keys.ENTER)
                    log.info(f"✓ Applied filter: Industry = {self.industry_filter}")
                    break
                except (TimeoutException, NoSuchElementException):
                    continue

            time.sleep(5)

        except Exception as e:
            log.warning(f"Could not apply industry filter automatically: {e}")
            log.warning("Please apply the filter manually in the browser")
            log.info("Waiting 10 seconds...")
            time.sleep(10)

    # ---- Step 4: Load Records ----

    def ensure_records_loaded(self, target_count: int = 5000):
        """Scroll to load records until target count is reached."""
        log.info("=" * 60)
        log.info(f"STEP 4: Loading records (target: {target_count})")
        log.info("=" * 60)

        last_count = 0
        no_change_iterations = 0
        max_no_change = 5

        while True:
            try:
                # Scroll to bottom
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                # Try to count rows
                row_selectors = [
                    (By.CSS_SELECTOR, "table tbody tr"),
                    (By.CSS_SELECTOR, "[role='row']"),
                    (By.CSS_SELECTOR, ".table-row"),
                ]

                current_count = 0
                for selector in row_selectors:
                    try:
                        rows = self.driver.find_elements(*selector)
                        current_count = len(rows)
                        if current_count > 0:
                            break
                    except NoSuchElementException:
                        continue

                log.info(f"Loaded records: {current_count} / {target_count}")

                if current_count >= target_count:
                    log.info(f"✓ Target reached: {current_count} records loaded")
                    break

                if current_count == last_count:
                    no_change_iterations += 1
                    if no_change_iterations >= max_no_change:
                        log.warning(f"No new records after {max_no_change} attempts")
                        log.warning(f"Stopping at {current_count} records")
                        break
                else:
                    no_change_iterations = 0

                last_count = current_count
                time.sleep(3)

            except Exception as e:
                log.error(f"Error while loading records: {e}")
                break

    # ---- Step 5: Download Records ----

    def download_records(self):
        """Download the filtered records as CSV."""
        log.info("=" * 60)
        log.info("STEP 5: Downloading records")
        log.info("=" * 60)

        try:
            self._download_via_export_button()
            return
        except Exception as e:
            log.info(f"Export button approach failed: {e}")

        try:
            self._download_via_more_menu()
            return
        except Exception as e:
            log.info(f"More menu approach failed: {e}")

        try:
            self._download_via_select_all()
            return
        except Exception as e:
            log.warning(f"All download strategies failed: {e}")
            self._take_screenshot("download_failed")

    def _download_via_export_button(self):
        export_selectors = [
            (By.XPATH, "//button[contains(., 'Export')]"),
            (By.XPATH, "//button[contains(., 'Download')]"),
            (By.CSS_SELECTOR, "[data-testid*='export']"),
            (By.XPATH, "//button[contains(@aria-label, 'Export')]"),
        ]
        for selector in export_selectors:
            try:
                btn = WebDriverWait(self.driver, 8).until(EC.element_to_be_clickable(selector))
                btn.click()
                time.sleep(2)
                log.info("Clicked export button")
                break
            except TimeoutException:
                continue
        else:
            raise NoSuchElementException("Export button not found")
        self._select_export_format_and_confirm()

    def _download_via_more_menu(self):
        more_selectors = [
            (By.XPATH, "//button[contains(., '...')]"),
            (By.XPATH, "//button[contains(., '⋮')]"),
            (By.CSS_SELECTOR, "[data-testid*='more']"),
            (By.CSS_SELECTOR, "[aria-label*='more' i]"),
            (By.CSS_SELECTOR, "[aria-label*='options' i]"),
        ]
        for selector in more_selectors:
            try:
                self.driver.find_element(*selector).click()
                time.sleep(1)
                break
            except NoSuchElementException:
                continue
        else:
            raise NoSuchElementException("More menu not found")

        for selector in [
            (By.XPATH, "//div[contains(text(), 'Export')]"),
            (By.XPATH, "//li[contains(text(), 'Export')]"),
            (By.XPATH, "//*[contains(text(), 'Download')]"),
        ]:
            try:
                self.driver.find_element(*selector).click()
                time.sleep(2)
                break
            except NoSuchElementException:
                continue
        self._select_export_format_and_confirm()

    def _download_via_select_all(self):
        for selector in [
            (By.CSS_SELECTOR, "th input[type='checkbox']"),
            (By.XPATH, "//input[@type='checkbox' and contains(@aria-label, 'select all')]"),
        ]:
            try:
                self.driver.find_element(*selector).click()
                time.sleep(1)
                log.info("Selected all rows")
                break
            except NoSuchElementException:
                continue
        self._download_via_export_button()

    def _select_export_format_and_confirm(self):
        time.sleep(1)
        for selector in [
            (By.XPATH, "//*[contains(text(), 'CSV')]"),
            (By.XPATH, "//label[contains(., 'CSV')]"),
        ]:
            try:
                self.driver.find_element(*selector).click()
                log.info("Selected CSV format")
                break
            except NoSuchElementException:
                continue

        for selector in [
            (By.XPATH, "//button[contains(text(), 'Export')]"),
            (By.XPATH, "//button[contains(text(), 'Download')]"),
            (By.XPATH, "//button[contains(text(), 'Confirm')]"),
        ]:
            try:
                self.driver.find_element(*selector).click()
                log.info("Confirmed download")
                break
            except NoSuchElementException:
                continue

        self._wait_for_download()

    def _wait_for_download(self, timeout=30):
        log.info(f"Waiting for download in: {self.download_dir}")
        deadline = time.time() + timeout
        while time.time() < deadline:
            files = list(Path(self.download_dir).glob("*"))
            completed = [
                f for f in files
                if f.suffix not in (".crdownload", ".part", ".tmp") and f.is_file()
            ]
            if completed:
                newest = max(completed, key=lambda f: f.stat().st_mtime)
                log.info(f"✓ Download complete: {newest.name}")
                return newest
            time.sleep(1)
        log.warning("Download may not have completed within timeout.")
        return None

    # ---- Utilities ----

    def _take_screenshot(self, name: str):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(self.download_dir, f"screenshot_{name}_{ts}.png")
        try:
            self.driver.save_screenshot(path)
            log.info(f"Screenshot saved: {path}")
        except Exception:
            pass

    # ---- Main Workflow ----

    def run(self):
        """Execute the full automation workflow."""
        log.info("=" * 60)
        log.info("CLAY.COM AUTOMATION - STARTING")
        log.info("=" * 60)
        log.info(f"Download directory: {self.download_dir}")
        log.info(f"Industry filter: {self.industry_filter}")
        log.info(f"Target records: {self.target_records}")

        try:
            # Step 0: Launch Chrome with debugging and attach Selenium
            self._launch_chrome_with_debugging()
            time.sleep(3)
            self.driver = self._attach_selenium()

            # Step 1: Verify we're logged into Clay
            if not self.verify_clay_session():
                log.error("=" * 60)
                log.error("AUTOMATION STOPPED: Not logged into Clay")
                log.error("=" * 60)
                log.error("ACTION REQUIRED:")
                log.error("1. Log into Clay in the Chrome window that just opened")
                log.error("2. Close Chrome completely")
                log.error("3. Run this script again")
                log.error("=" * 60)
                return

            # Step 2-5: Navigate, filter, load records, download
            self.navigate_to_table()
            self.filter_by_industry()
            self.ensure_records_loaded(target_count=self.target_records)
            self.download_records()

            log.info("=" * 60)
            log.info("✓ AUTOMATION COMPLETE!")
            log.info("=" * 60)
            log.info(f"Check your downloads at: {self.download_dir}")

        except KeyboardInterrupt:
            log.info("Automation interrupted by user")
        except Exception as e:
            log.error("=" * 60)
            log.error(f"AUTOMATION FAILED: {e}")
            log.error("=" * 60)
            if self.driver:
                self._take_screenshot("fatal_error")
            raise
        finally:
            # Detach Selenium but DON'T close Chrome (user may want to keep using it)
            if self.driver:
                log.info("Keeping Chrome open for you to review...")
                log.info("Detaching Selenium in 5 seconds...")
                time.sleep(5)
                try:
                    self.driver.quit()
                except Exception:
                    pass
                log.info("✓ Selenium detached. Chrome is still running.")


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 70)
    print("CLAY AUTOMATION SCRIPT - FIXED VERSION")
    print("=" * 70)
    print()
    
    # ── Customize these values ──────────────────────────────────────────

    # Your Chrome executable path (from chrome://version → Executable Path)
    CHROME = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

    # Your Chrome profile (from chrome://version → Profile Path)
    PROFILE_DIR = r"C:\Users\hp\AppData\Local\Google\Chrome\User Data"
    PROFILE_NAME = "Profile 1"

    # Optional: go directly to a specific workspace URL
    # (copy it from your browser address bar when on Clay)
    # e.g. "https://app.clay.com/workspaces/744216/home"
    CLAY_URL = None

    # Filter & download settings
    INDUSTRY = "Technology"      # Change to: "Healthcare", "Finance", etc.
    RECORDS = 5000               # Records to load before downloading
    DOWNLOAD_DIR = DEFAULT_DOWNLOAD_DIR
    # ────────────────────────────────────────────────────────────────────

    print(f"Chrome Path: {CHROME}")
    print(f"Profile: {PROFILE_DIR}\\{PROFILE_NAME}")
    print(f"Debug Port: {REMOTE_DEBUG_PORT}")
    print(f"Downloads: {DOWNLOAD_DIR}")
    print()
    print("IMPORTANT: Make sure Chrome is completely closed before continuing!")
    print("Check Task Manager if needed.")
    print()
    
    input("Press ENTER to start automation...")
    print()

    automation = ClayAutomation(
        chrome_exe=CHROME,
        chrome_profile_dir=PROFILE_DIR,
        chrome_profile_name=PROFILE_NAME,
        download_dir=DOWNLOAD_DIR,
        industry_filter=INDUSTRY,
        target_records=RECORDS,
        clay_workspace_url=CLAY_URL,
    )
    automation.run()