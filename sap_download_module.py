#!/usr/bin/env python3
"""
SAP BusinessObjects OpenDocument Download Module using Selenium
Handles authentication and file downloads from SAP BOE using browser automation
"""

import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict
import os
import shutil
import glob
import uuid
import tempfile
import fcntl
import atexit
import psutil
import subprocess

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException

logger = logging.getLogger(__name__)


class SAPOpenDocumentDownloader:
    """Download files from SAP BusinessObjects OpenDocument URLs using Selenium"""
    
    # Class-level lock file for Chrome instance coordination
    # Use a location where etluser has write permissions
    CHROME_LOCK_FILE = '/logs/sap_chrome_driver.lock'
    
    @classmethod
    def cleanup_stale_locks(cls):
        """Clean up stale lock files from crashed processes"""
        try:
            if os.path.exists(cls.CHROME_LOCK_FILE):
                # Check if lock file is stale (older than 5 minutes)
                file_age = time.time() - os.path.getmtime(cls.CHROME_LOCK_FILE)
                if file_age > 300:  # 5 minutes
                    try:
                        os.unlink(cls.CHROME_LOCK_FILE)
                        logger.info("Removed stale Chrome lock file")
                    except PermissionError:
                        # Try to remove with sudo if running as root
                        try:
                            subprocess.run(['sudo', 'rm', '-f', cls.CHROME_LOCK_FILE], capture_output=True)
                            logger.info("Removed stale Chrome lock file with sudo")
                        except:
                            logger.warning(f"Could not remove stale lock file {cls.CHROME_LOCK_FILE} - permission denied")
        except Exception as e:
            logger.debug(f"Error cleaning up stale locks: {e}")
        
        # No need to clean up Chrome user data directories since we're not using them
    
    def __init__(self, config: Dict):
        """
        Initialize downloader with configuration
        
        Args:
            config: Dictionary with keys:
                - username: SAP BOE username
                - password: SAP BOE password
                - timeout: Request timeout in seconds (default: 300)
                - download_dir: Directory for downloads (default: /tmp/downloads)
                - headless: Run Chrome in headless mode (default: True)
                - lookback_timeout: Extended timeout for lookback files (default: 600)
        """
        # Clean up any stale lock files
        self.cleanup_stale_locks()
        
        self.username = config.get('username', 'sduggan')
        self.password = config.get('password', 'sduggan')
        self.timeout = config.get('timeout', 300)
        self.lookback_timeout = config.get('lookback_timeout', 600)  # 10 minutes for lookback files
        self.download_dir = Path(config.get('download_dir', '/tmp/downloads'))
        self.headless = config.get('headless', True)
        
        # Create download directory
        self.download_dir.mkdir(exist_ok=True, parents=True)
        
        # URLs from configuration - now supports dynamic URL loading
        # Check if URLs are provided in config, otherwise use defaults
        if 'sap_urls' in config:

            # Normalize URL keys to handle both uppercase and lowercase

            self.urls = {}

            for key, value in config['sap_urls'].items():

                # Store with uppercase key for consistency

                self.urls[key.upper()] = value
        else:
            # Default URLs for backward compatibility
            self.urls = {
                'AMRS': 'https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AYscKsmnmVFMgwa4u8GO5GU&sOutputFormat=E',
                'EMEA': 'https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AXFSzkEFSQpOrrU9_35AhpQ&sOutputFormat=E',
                'AMRS_30DAYS': 'https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AXmFuFTG4DBBrefomiwL1aE&sOutputFormat=E',
                'EMEA_30DAYS': 'https://www.mfanalyzer.com/BOE/OpenDocument/opendoc/openDocument.jsp?sIDType=CUID&iDocID=AQbKBz8wx0pHojHl0uBm2sw&sOutputFormat=E'
            }
        
        self.driver = None
        self.wait = None
        self._logged_in = False
        self.user_data_dir = None
        self._lock_file = None
    
    def _setup_driver(self):
        """Setup Chrome driver with download preferences"""
        if self.driver:
            return
        
        # Acquire lock to prevent concurrent Chrome instances
        lock_file = None
        max_wait_time = 30  # Maximum time to wait for lock
        start_time = time.time()
        
        try:
            # Ensure lock directory exists and has proper permissions
            lock_dir = os.path.dirname(self.CHROME_LOCK_FILE)
            if not os.path.exists(lock_dir):
                os.makedirs(lock_dir, mode=0o777, exist_ok=True)
            
            # Try to acquire exclusive lock with proper error handling
            lock_file = None
            attempts = 0
            while attempts < 3:
                try:
                    # Open lock file with explicit permissions
                    lock_file = open(self.CHROME_LOCK_FILE, 'w')
                    # Set file permissions to be writable by all
                    os.chmod(self.CHROME_LOCK_FILE, 0o666)
                    break
                except PermissionError as e:
                    attempts += 1
                    logger.warning(f"Permission error opening lock file (attempt {attempts}): {e}")
                    # If file exists and we can't open it, try to remove it
                    if os.path.exists(self.CHROME_LOCK_FILE):
                        try:
                            # Check if file is stale
                            file_age = time.time() - os.path.getmtime(self.CHROME_LOCK_FILE)
                            if file_age > 60:  # 1 minute
                                # Use subprocess to remove with sudo if needed
                                result = subprocess.run(['sudo', 'rm', '-f', self.CHROME_LOCK_FILE], 
                                                      capture_output=True, text=True)
                                if result.returncode == 0:
                                    logger.info("Removed stale lock file with sudo")
                                    time.sleep(0.1)
                                    continue
                        except:
                            pass
                    if attempts >= 3:
                        raise
                    time.sleep(0.5)
            
            if not lock_file:
                raise Exception("Failed to create lock file after 3 attempts")
            
            while True:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                    # Write PID to lock file for debugging
                    lock_file.write(f"{os.getpid()}\n")
                    lock_file.flush()
                    logger.info(f"Acquired Chrome lock (PID: {os.getpid()})")
                    break
                except IOError:
                    if time.time() - start_time > max_wait_time:
                        # Check if the process holding the lock is still alive
                        try:
                            with open(self.CHROME_LOCK_FILE, 'r') as f:
                                lock_pid = int(f.read().strip())
                            if not psutil.pid_exists(lock_pid):
                                logger.warning(f"Lock held by dead process {lock_pid}, removing lock")
                                try:
                                    os.unlink(self.CHROME_LOCK_FILE)
                                except PermissionError:
                                    subprocess.run(['sudo', 'rm', '-f', self.CHROME_LOCK_FILE], 
                                                 capture_output=True)
                                time.sleep(0.1)
                                continue
                        except:
                            pass
                        raise TimeoutException("Timeout waiting for Chrome lock")
                    logger.info("Waiting for Chrome lock...")
                    time.sleep(0.5)
            
            # Add delay to ensure any previous Chrome instance is fully terminated
            time.sleep(2)
            
            chrome_options = Options()
            
            # Headless mode for containers
            if self.headless:
                chrome_options.add_argument("--headless=new")  # Use new headless mode for better download support
                chrome_options.add_argument("--disable-gpu")
            
            # Required for Docker
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            # Enable downloads in headless mode
            chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
            chrome_options.add_argument("--enable-features=DownloadBubble,DownloadBubbleV2")
        
            # Disable images and CSS for faster loading (optional)
            chrome_prefs = {
                "profile.default_content_settings": {"images": 2},
                "profile.managed_default_content_settings": {"images": 2}
            }
            
            # Set download directory
            chrome_prefs.update({
                "download.default_directory": str(self.download_dir),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "safebrowsing.disable_download_protection": True,
                "plugins.always_open_pdf_externally": True,
                "profile.default_content_setting_values.automatic_downloads": 1
            })
            
            chrome_options.add_experimental_option("prefs", chrome_prefs)
            
            # Additional options for stability
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--allow-running-insecure-content")
            chrome_options.add_argument("--window-size=1920,1080")
        
            # Don't use user data directory to avoid conflicts
            # Chrome in headless mode doesn't need profile persistence for downloads
            self.user_data_dir = None
            
            # Basic isolation options
            chrome_options.add_argument("--disable-background-networking")
            chrome_options.add_argument("--disable-sync")
            chrome_options.add_argument("--disable-translate")
            chrome_options.add_argument("--disable-default-apps")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-gpu-sandbox")
            chrome_options.add_argument("--disable-software-rasterizer")
            chrome_options.add_argument("--disable-background-timer-throttling")
            chrome_options.add_argument("--disable-backgrounding-occluded-windows")
            chrome_options.add_argument("--disable-renderer-backgrounding")
            chrome_options.add_argument("--disable-features=TranslateUI")
            chrome_options.add_argument("--disable-ipc-flooding-protection")
            
            # Memory optimization
            chrome_options.add_argument("--memory-pressure-off")
            chrome_options.add_argument("--max_old_space_size=4096")
            
            # Disable crash reporting and telemetry
            chrome_options.add_argument("--disable-breakpad")
            chrome_options.add_argument("--disable-client-side-phishing-detection")
            chrome_options.add_argument("--disable-cloud-import")
            chrome_options.add_argument("--disable-popup-blocking")
            chrome_options.add_argument("--disable-print-preview")
            chrome_options.add_argument("--disable-zero-browsers-open-for-tests")
            
            # Use random port for debugging to avoid conflicts
            chrome_options.add_argument("--remote-debugging-port=0")
            
            # Create driver
            try:
                service = Service('/usr/local/bin/chromedriver')
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                self.wait = WebDriverWait(self.driver, 30)
                
                # Enable downloads in headless Chrome
                if self.headless:
                    params = {'behavior': 'allow', 'downloadPath': str(self.download_dir)}
                    self.driver.execute_cdp_cmd('Page.setDownloadBehavior', params)
                
                logger.info(f"Chrome driver initialized successfully with user data dir: {self.user_data_dir}")
                
                # Store lock file reference to release later
                self._lock_file = lock_file
                
            except Exception as e:
                logger.error(f"Failed to initialize Chrome driver: {e}")
                # Clean up user data directory on failure
                if self.user_data_dir and os.path.exists(self.user_data_dir):
                    try:
                        shutil.rmtree(self.user_data_dir, ignore_errors=True)
                    except:
                        pass
                raise
                
        except Exception as e:
            # Release lock on any error
            if lock_file:
                try:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                    lock_file.close()
                    logger.info("Released Chrome lock due to error")
                except:
                    pass
            raise
    
    def _login_to_bi(self) -> bool:
        """Login to SAP BI Launch Pad"""
        if self._logged_in:
            return True
            
        logger.info("Attempting login to SAP BI Launch Pad...")
        
        try:
            # Go to BI Launch Pad
            self.driver.get("https://www.mfanalyzer.com/BOE/BI")
            time.sleep(2)
            
            # Check if we need to login
            if "logon" in self.driver.current_url.lower():
                logger.info("Login page detected, entering credentials...")
                
                # Wait for and fill username
                username_field = self.wait.until(
                    EC.presence_of_element_located((By.ID, "_id0:logon:USERNAME"))
                )
                username_field.clear()
                username_field.send_keys(self.username)
                
                # Fill password
                password_field = self.driver.find_element(By.ID, "_id0:logon:PASSWORD")
                password_field.clear()
                password_field.send_keys(self.password)
                
                # Click login button
                login_button = self.driver.find_element(By.ID, "_id0:logon:logonButton")
                login_button.click()
                
                # Wait for login to complete
                time.sleep(5)
                
                # Check if login successful
                if "logon" not in self.driver.current_url.lower():
                    logger.info("✓ Login successful!")
                    self._logged_in = True
                    return True
                else:
                    logger.error("✗ Login failed - still on login page")
                    return False
            else:
                logger.info("Already logged in or no login required")
                self._logged_in = True
                return True
                
        except TimeoutException:
            logger.error("Login timeout - page elements not found")
            return False
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False
    
    def download_file(self, region: str, target_date: datetime, output_dir: Path) -> Optional[str]:
        """
        Download file for specified region and date
        
        Args:
            region: 'AMRS', 'EMEA', 'AMRS_30DAYS', or 'EMEA_30DAYS'
            target_date: Date for the data
            output_dir: Directory to save the file
            
        Returns:
            Path to downloaded file or None if failed
        """
        # Normalize region to uppercase for consistency

        region_upper = region.upper()

        

        if region_upper not in self.urls:
            logger.error(f"Unknown region: {region} (normalized: {region_upper})")
            logger.error(f"Available regions: {list(self.urls.keys())}")
            return None
        
        # Setup driver with retry logic
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                self._setup_driver()
                break
            except Exception as e:
                logger.warning(f"Chrome initialization attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    # Cleanup before retry
                    self.close()
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                else:
                    logger.error("Failed to initialize Chrome after all retries")
                    raise
        
        # Login if not already logged in
        if not self._login_to_bi():
            logger.error("Failed to login to BI")
            return None
        
        url = self.urls[region_upper]
        filename = f"DataDump__{region}_{target_date.strftime('%Y%m%d')}.xlsx"
        final_path = output_dir / filename
        
        # Determine timeout based on file type
        is_lookback = '30DAYS' in region_upper
        download_timeout = self.lookback_timeout if is_lookback else self.timeout
        
        logger.info(f"Downloading {region} file for {target_date.strftime('%Y-%m-%d')}...")
        if is_lookback:
            logger.info(f"Using extended timeout of {download_timeout} seconds for lookback file")
        
        try:
            # Clear download directory
            for file in self.download_dir.glob("*.xlsx"):
                file.unlink()
            
            # Navigate to OpenDocument URL
            self.driver.get(url)
            
            # Wait for page to load
            time.sleep(3)
            
            # Check if we got an OpenDocument login form
            try:
                od_username = self.driver.find_element(By.ID, "_id0:logon:USERNAME")
                
                logger.info("OpenDocument login form detected, logging in...")
                
                # Fill credentials
                od_username.clear()
                od_username.send_keys(self.username)
                
                od_password = self.driver.find_element(By.ID, "_id0:logon:PASSWORD")
                od_password.clear()
                od_password.send_keys(self.password)
                
                # Click login
                od_login_btn = self.driver.find_element(By.ID, "_id0:logon:logonButton")
                od_login_btn.click()
                
                time.sleep(5)
                
            except NoSuchElementException:
                # No login form, file might download directly
                logger.info("No OpenDocument login form found, checking for download...")
            
            # Wait for download to complete with appropriate timeout
            download_complete = self._wait_for_download(timeout=download_timeout)
            
            if download_complete:
                # Find the downloaded file
                excel_files = list(self.download_dir.glob("*.xlsx"))
                if excel_files:
                    downloaded_file = excel_files[0]
                    
                    # Move to final location
                    output_dir.mkdir(exist_ok=True, parents=True)
                    shutil.move(str(downloaded_file), str(final_path))
                    
                    logger.info(f"✓ Downloaded {region}: {final_path} ({final_path.stat().st_size:,} bytes)")
                    return str(final_path)
                else:
                    logger.error(f"Download completed but no Excel file found for {region}")
                    return None
            else:
                logger.error(f"✗ Download timeout for {region} after {download_timeout} seconds")
                self._save_debug_screenshot(f"download_timeout_{region}.png")
                return None
                
        except Exception as e:
            logger.error(f"Download error for {region}: {e}")
            self._save_debug_screenshot(f"download_error_{region}.png")
            return None
    
    def _wait_for_download(self, timeout: int = None) -> bool:
        """Wait for download to complete"""
        if timeout is None:
            timeout = self.timeout
            
        logger.info(f"Waiting for download to complete (timeout: {timeout}s)...")
        
        for i in range(timeout):
            # Check for Excel files
            excel_files = list(self.download_dir.glob("*.xlsx"))
            
            # Check for temp Chrome download files
            temp_files = list(self.download_dir.glob("*.crdownload"))
            
            if excel_files and not temp_files:
                # Download complete
                time.sleep(1)  # Extra wait to ensure file is fully written
                return True
            
            time.sleep(1)
            
            if i % 10 == 0 and i > 0:
                logger.debug(f"Still waiting... ({i}/{timeout}s)")
        
        return False
    
    def _save_debug_screenshot(self, filename: str):
        """Save screenshot for debugging"""
        try:
            screenshot_path = Path("/logs") / filename
            self.driver.save_screenshot(str(screenshot_path))
            logger.info(f"Debug screenshot saved: {screenshot_path}")
        except Exception as e:
            logger.error(f"Failed to save screenshot: {e}")
    
    def test_connectivity(self) -> Dict[str, bool]:
        """
        Test connectivity to SAP OpenDocument URLs
        
        Returns:
            Dictionary with connectivity status for each region
        """
        self._setup_driver()
        results = {}
        
        # Test all configured URLs
        for region_key, url in self.urls.items():
            # Create a display name for the region
            display_name = region_key.upper().replace('_', ' ')
            
            try:
                logger.info(f"Testing connectivity to {display_name} URL...")
                
                self.driver.get(url)
                time.sleep(3)
                
                # Check page title or content
                if "logon" in self.driver.current_url.lower():
                    results[display_name] = True
                    logger.info(f"{display_name}: Login page accessible ✓")
                elif self.driver.title:
                    results[display_name] = True
                    logger.info(f"{display_name}: Page accessible ✓")
                else:
                    results[display_name] = False
                    logger.warning(f"{display_name}: Unknown response")
                    
            except Exception as e:
                results[display_name] = False
                logger.error(f"{display_name}: Connection failed - {str(e)}")
        
        return results
    
    def close(self):
        """Close the browser and cleanup temporary directories"""
        if self.driver:
            try:
                # Get the Chrome process PID before quitting
                chrome_pid = None
                try:
                    chrome_pid = self.driver.service.process.pid
                except:
                    pass
                
                self.driver.quit()
                logger.info("Browser closed")
                
                # Ensure Chrome process is terminated
                if chrome_pid:
                    try:
                        parent = psutil.Process(chrome_pid)
                        children = parent.children(recursive=True)
                        for child in children:
                            try:
                                child.terminate()
                            except:
                                pass
                        parent.terminate()
                        # Give processes time to terminate
                        time.sleep(1)  # Increased from 0.5
                    except psutil.NoSuchProcess:
                        pass  # Process already terminated
                    except Exception as e:
                        logger.debug(f"Error terminating Chrome processes: {e}")
                
                # No need for aggressive Chrome process cleanup since we're not using user data directories
                        
            except Exception as e:
                logger.error(f"Error closing browser: {e}")
            finally:
                self.driver = None
                self.wait = None
                self._logged_in = False
        
        # No user data directory to clean up since we're not using one
        self.user_data_dir = None
        
        # Release Chrome lock
        if self._lock_file:
            try:
                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
                self._lock_file.close()
                logger.info("Released Chrome lock")
                # Try to remove lock file
                try:
                    os.unlink(self.CHROME_LOCK_FILE)
                except PermissionError:
                    # Use sudo if needed
                    subprocess.run(['sudo', 'rm', '-f', self.CHROME_LOCK_FILE], capture_output=True)
            except Exception as e:
                logger.debug(f"Error releasing Chrome lock: {e}")
            finally:
                self._lock_file = None
        
        # Add delay to ensure full cleanup before next Chrome instance
        time.sleep(3)  # Increased to ensure Chrome is fully terminated


# Backward compatibility wrapper
def download_with_selenium(region: str, target_date: datetime, output_dir: Path, config: Dict) -> Optional[str]:
    """
    Download file using Selenium (for use in fund_etl_pipeline.py)
    
    This function provides backward compatibility with the existing ETL pipeline
    """
    downloader = SAPOpenDocumentDownloader(config)
    
    try:
        return downloader.download_file(region, target_date, output_dir)
    finally:
        downloader.close()


if __name__ == "__main__":
    # Test the downloader
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')
    
    config = {
        'username': 'sduggan',
        'password': 'sduggan',
        'download_dir': '/tmp/test_downloads',
        'headless': True,
        'lookback_timeout': 600  # 10 minutes for lookback files
    }
    
    downloader = SAPOpenDocumentDownloader(config)
    
    try:
        # Test connectivity
        results = downloader.test_connectivity()
        print("\nConnectivity test results:")
        for region, status in results.items():
            print(f"  {region}: {'✓' if status else '✗'}")
        
        # Test download
        output_dir = Path('/tmp')
        target_date = datetime.now()
        
        for region in ["AMRS", "EMEA", "AMRS_30DAYS", "EMEA_30DAYS"]:
            filepath = downloader.download_file(region, target_date, output_dir)
            if filepath:
                print(f"\nSuccessfully downloaded {region}: {filepath}")
            else:
                print(f"\nFailed to download {region}")
                
    finally:
        downloader.close()