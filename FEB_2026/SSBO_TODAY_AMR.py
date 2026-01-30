import os
import re
import sys
import pytz
import time
import json
import atexit
import shutil
import zipfile
import logging
import requests
import subprocess
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from colorama import Fore, Style
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime, timedelta, timezone
from google_auth_oauthlib.flow import InstalledAppFlow
from playwright.sync_api import sync_playwright, expect

# Load MongoDB API Key 
load_dotenv("/Users/nera_thomas/Desktop/Telemarketing/api/mongodb/.env")
MONGODB_URI = os.getenv("MONGODB_API_KEY")

# Logging configuration
LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("log_files")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(LOG_DIR / "RETENTION_errors.log", encoding="utf-8")
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

def safe_call(func, *args, description=None, retries=500, delay=60, **kwargs):
    """
    Call a function safely with retries.

    Parameters:
        func: callable to execute.
        description: Optional string used in logs for readability.
        retries: Number of attempts before giving up.
        delay: Seconds to wait between retries.
    """
    attempt = 1
    label = description or getattr(func, "__name__", "callable")
    while True:
        try:
            return func(*args, **kwargs)
        except KeyboardInterrupt:
            raise
        except Exception:
            logger.exception("Error during %s (attempt %s/%s)", label, attempt, retries)
            if attempt >= retries:
                logger.error("Giving up on %s after %s attempts.", label, retries)
                return None
            attempt += 1
            time.sleep(delay)

# Chrome Settings
class Automation:

    # Chrome CDP 
    chrome_proc = None
    @classmethod
    def chrome_CDP(cls):

        # User Profile
        USER_DATA_DIR = f"/Users/nera_thomas/Library/Application Support/Google/Chrome/Profile 15"
        
        # Start Chrome normally
        cls.chrome_proc = subprocess.Popen([
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "--remote-debugging-port=9333",
            "--disable-session-crashed-bubble",
            "--hide-crash-restore-bubble",
            "--no-first-run",
            "--no-default-browser-check",
            f"--user-data-dir={USER_DATA_DIR}",  # User Profile
            "--headless=new",                    # ------> if want to use headless mode, use --windows-size together, due to headless mode small screen size
            "--window-size=1920,1080",           # ✅ simulate full HD
            "--force-device-scale-factor=1",     # ✅ ensure no zoom scalin
        ],
        stdout=subprocess.DEVNULL,  # ✅ hide chrome cdp logs
        stderr=subprocess.DEVNULL   # ✅ hide chrome cdp logs
        )
    
        # wait for Chrome CDP launch...
        cls.wait_for_cdp_ready()

        atexit.register(cls.cleanup)

    # Close Chrome CDP
    @classmethod
    def cleanup(cls):
        try:
            cls.chrome_proc.terminate()
        except Exception as e:
            print(f"Error terminating Chrome: {e}")
    
    # Wait for Chrome CDP to be ready
    @staticmethod
    def wait_for_cdp_ready(timeout=10):
        """Wait until Chrome CDP is ready at http://localhost:9333/json"""
        for _ in range(timeout):
            try:
                res = requests.get("http://localhost:9333/json")
                if res.status_code == 200:
                    return True
            except:
                pass
            time.sleep(1)
        raise RuntimeError("Chrome CDP is not ready after waiting.")

# BO Account
class BO_Account:

   # Load the .env file
   # Format to use in other Class ===>             cls.accounts["gc99"]["merchant_code"]
    load_dotenv()

    accounts = {
        "super_swan": {
            "acc_ID": os.getenv("ACC_ID_SUPERSWAN"),
            "acc_PASS": os.getenv("ACC_PASS_SUPERSWAN")
        },
        "super_swan2": {
            "acc_ID": os.getenv("ACC_ID_SUPERSWAN2"),
            "acc_PASS": os.getenv("ACC_PASS_SUPERSWAN2")
        },
        "wdb1": {
            "acc_ID": os.getenv("ACC_ID_WDB1"),
            "acc_PASS": os.getenv("ACC_PASS_WDB1")
        },
        "22fun": {
            "merchant_code": os.getenv("MERCHANT_CODE_22FUN"),
            "acc_ID": os.getenv("ACC_ID_22FUN"),
            "acc_PASS": os.getenv("ACC_PASS_22FUN")
        },
        "gc99": {
            "merchant_code": os.getenv("MERCHANT_CODE_GC99"),
            "acc_ID": os.getenv("ACC_ID_GC99"),
            "acc_PASS": os.getenv("ACC_PASS_GC99")
        },
        "828": {
            "merchant_code": os.getenv("MERCHANT_CODE_828"),
            "acc_ID": os.getenv("ACC_ID_828"),
            "acc_PASS": os.getenv("ACC_PASS_828")
        },
        "jw8": {
            "merchant_code": os.getenv("MERCHANT_CODE_JW8"),
            "acc_ID": os.getenv("ACC_ID_JW8"),
            "acc_PASS": os.getenv("ACC_PASS_JW8")
        },
        "n191": {
            "merchant_code": os.getenv("MERCHANT_CODE_N191"),
            "acc_ID": os.getenv("ACC_ID_N191"),
            "acc_PASS": os.getenv("ACC_PASS_N191")
        },
        "slot": {
            "merchant_code": os.getenv("MERCHANT_CODE_SLOT"),
            "acc_ID": os.getenv("ACC_ID_SLOT"),
            "acc_PASS": os.getenv("ACC_PASS_SLOT")
        },
        "s345": {
            "merchant_code": os.getenv("MERCHANT_CODE_S345"),
            "acc_ID": os.getenv("ACC_ID_S345"),
            "acc_PASS": os.getenv("ACC_PASS_S345")
        },
        "s369": {
            "merchant_code": os.getenv("MERCHANT_CODE_S369"),
            "acc_ID": os.getenv("ACC_ID_S369"),
            "acc_PASS": os.getenv("ACC_PASS_S369")
        },
        "s66": {
            "merchant_code": os.getenv("MERCHANT_CODE_S66"),
            "acc_ID": os.getenv("ACC_ID_S66"),
            "acc_PASS": os.getenv("ACC_PASS_S66")
        },
        "s855": {
            "merchant_code": os.getenv("MERCHANT_CODE_S855"),
            "acc_ID": os.getenv("ACC_ID_S855"),
            "acc_PASS": os.getenv("ACC_PASS_S855")
        },
        "s55": {
            "merchant_code": os.getenv("MERCHANT_CODE_S55"),
            "acc_ID": os.getenv("ACC_ID_S55"),
            "acc_PASS": os.getenv("ACC_PASS_S55")
        },
        "aw8": {
            "merchant_code": os.getenv("MERCHANT_CODE_AW8"),
            "acc_ID": os.getenv("ACC_ID_AW8"),
            "acc_PASS": os.getenv("ACC_PASS_AW8")
        },
        "n789": {
            "merchant_code": os.getenv("MERCHANT_CODE_N789"),
            "acc_ID": os.getenv("ACC_ID_N789"),
            "acc_PASS": os.getenv("ACC_PASS_N789")
        },
        "s212": {
            "merchant_code": os.getenv("MERCHANT_CODE_S212"),
            "acc_ID": os.getenv("ACC_ID_S212"),
            "acc_PASS": os.getenv("ACC_PASS_S212")
        },
        "mf191": {
            "merchant_code": os.getenv("MERCHANT_CODE_MF191"),
            "acc_ID": os.getenv("ACC_ID_MF191"),
            "acc_PASS": os.getenv("ACC_PASS_MF191")
        },
        "n855": {
            "merchant_code": os.getenv("MERCHANT_CODE_N855"),
            "acc_ID": os.getenv("ACC_ID_N855"),
            "acc_PASS": os.getenv("ACC_PASS_N855")
        },
        "g855": {
            "merchant_code": os.getenv("MERCHANT_CODE_G855"),
            "acc_ID": os.getenv("ACC_ID_G855"),
            "acc_PASS": os.getenv("ACC_PASS_G855")
        },
        "g345": {
            "merchant_code": os.getenv("MERCHANT_CODE_G345"),
            "acc_ID": os.getenv("ACC_ID_G345"),
            "acc_PASS": os.getenv("ACC_PASS_G345")
        },
        "r66": {
            "merchant_code": os.getenv("MERCHANT_CODE_R66"),
            "acc_ID": os.getenv("ACC_ID_R66"),
            "acc_PASS": os.getenv("ACC_PASS_R66")
        },
        "r99": {
            "merchant_code": os.getenv("MERCHANT_CODE_R99"),
            "acc_ID": os.getenv("ACC_ID_R99"),
            "acc_PASS": os.getenv("ACC_PASS_R99")
        },
        "22W": {
            "merchant_code": os.getenv("MERCHANT_CODE_22W"),
            "acc_ID": os.getenv("ACC_ID_22W"),
            "acc_PASS": os.getenv("ACC_PASS_22W")
        },
        "i88": {    
            "merchant_code": os.getenv("MERCHANT_CODE_I88"),
            "acc_ID": os.getenv("ACC_ID_I88"),
            "acc_PASS": os.getenv("ACC_PASS_I88")
        },
        "k88": {
            "merchant_code": os.getenv("MERCHANT_CODE_K88"),
            "acc_ID": os.getenv("ACC_ID_K88"),
            "acc_PASS": os.getenv("ACC_PASS_K88")
        },
        "jaya11": {
            "merchant_code": os.getenv("MERCHANT_CODE_JAYA11"),
            "acc_ID": os.getenv("ACC_ID_JAYA11"),
            "acc_PASS": os.getenv("ACC_PASS_JAYA11")
        },
        "joli": {
            "merchant_code": os.getenv("MERCHANT_CODE_JOLI"),
            "acc_ID": os.getenv("ACC_ID_JOLI"),
            "acc_PASS": os.getenv("ACC_PASS_JOLI")
        },
        "uea8": {
            "merchant_code": os.getenv("MERCHANT_CODE_UEA8"),
            "acc_ID": os.getenv("ACC_ID_UEA8"),
            "acc_PASS": os.getenv("ACC_PASS_UEA8")
        },
    }

# MongoDB <-> Google Sheet
class mongodb_2_gs:

    # Handles Google Sheets operations for the Telemarketing project.
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    TOKEN_PATH = "./api/google/token.json"
    CREDS_PATH = "./api/google/credentials.json"

    # Google API Authentication
    @classmethod
    def googleAPI(cls):
        """Authenticate and return Google credentials."""
        creds = None

        # Load token if exists
        if os.path.exists(cls.TOKEN_PATH):
            creds = Credentials.from_authorized_user_file(cls.TOKEN_PATH, cls.SCOPES)

        # Refresh or create new credentials if invalid
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(cls.CREDS_PATH, cls.SCOPES)
                creds = flow.run_local_server(port=0)

            # Save refreshed credentials
            with open(cls.TOKEN_PATH, "w") as token:
                token.write(creds.to_json())

        return creds

    @staticmethod
    def _quote_sheet_title(sheet_title):
        """Wrap sheet/tab names that contain spaces/special chars in single quotes."""
        if sheet_title is None:
            raise ValueError("Sheet/tab name cannot be empty.")
        title = str(sheet_title).strip()
        if not title:
            raise ValueError("Sheet/tab name cannot be empty.")
        if title.startswith("'") and title.endswith("'"):
            return title
        safe_title = title.replace("'", "''")
        return f"'{safe_title}'"

    @classmethod
    def _build_a1_range(cls, sheet_title, start_column, start_row=None, end_column=None, end_row=None):
        """Construct an A1 notation range with safe sheet quoting."""
        if not start_column:
            raise ValueError("Start column is required for A1 range.")
        start_col = str(start_column).strip()
        if not start_col:
            raise ValueError("Start column is required for A1 range.")
        start_part = f"{start_col}{start_row or ''}"
        sheet_ref = cls._quote_sheet_title(sheet_title)
        if end_column:
            end_col = str(end_column).strip()
            if not end_col:
                raise ValueError("End column is required when provided.")
            end_part = f"{end_col}{end_row or ''}"
            return f"{sheet_ref}!{start_part}:{end_part}"
        return f"{sheet_ref}!{start_part}"

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO & IBS AMR =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

    # Unzip File (ssbo)
    @classmethod
    def unzip(cls, file_name):
        base_dir = "/Users/nera_thomas/Desktop/Telemarketing/excel_file"
        zip_path = os.path.join(base_dir, f"{file_name}.zip")

        if not os.path.exists(zip_path):
            print(f"❌ Zip file not found: {zip_path}")
            return

        # ✅ Use dedicated temp folder
        extract_to = os.path.join(base_dir, f"{file_name}_temp")
        os.makedirs(extract_to, exist_ok=True)

        new_csv_path = os.path.join(base_dir, f"{file_name}.csv")
        if os.path.exists(new_csv_path):
            os.remove(new_csv_path)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"✅ Unzipped into folder: {extract_to}")

        csv_files = []
        for root, _, files in os.walk(extract_to):
            for file in files:
                if file.endswith(".csv"):
                    csv_files.append(os.path.join(root, file))

        if not csv_files:
            print("❌ No CSV found inside ZIP.")
            shutil.rmtree(extract_to, ignore_errors=True)
            return

        shutil.move(csv_files[0], new_csv_path)
        shutil.rmtree(extract_to, ignore_errors=True)

    # MongoDB Database (All Member Report)
    def mongodbAPI_AMR(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["RETENTION_0226"]
        collection = db[collection]

        # Drop any legacy indexes that might conflict
        for idx in (
            "player_id_1_amount_1_completed_at_1",
            "all_member_unique",
            "username_1_first_name_1_register_info_date_1",
        ):
            try:
                collection.drop_index(idx)
            except Exception:
                pass

        # Ensure username+member_id+register_date unique to avoid duplicates
        collection.create_index(
            [("username", 1), ("fullname", 1), ("member_id", 1)],
            name="all_member_unique",
            unique=True,
            partialFilterExpression={
                "username": {"$type": "string"},
                "fullname": {"$type": "string"},
                "member_id": {"$type": "string"},
            },
        )

        inserted = 0
        skipped = 0
        cleaned_docs = []

        ordered_fields = [
            "none",
            "currency",
            "member_group",
            "username",
            "fullname",
            "member_id",
            "phone_no",
            "email",
            "vip_level",
            "aff_username",
            "member_tag",
            "deposit_count",
            "affiliate_transfer_in_count",
            "affiliate_transfer_in_amount",
            "total_deposit",
            "processing_fee",
            "received_deposit",
            "withdraw_count",
            "affiliate_transfer_out_count",
            "affiliate_transfer_out_amount",
            "total_withdraw",
            "adjust_count",
            "adjust_in",
            "adjust_out",
            "total_adjust",
            "bet_count",
            "total_valid_bet",
            "total_bonus_bet",
            "total_turnover",
            "total_valid_bonus_bet",
            "win_loss",
            "player_bonus_winloss",
            "total_bonus",
            "total_cancel_bonus",
            "total_rebate",
            "total_referral",
            "total_referral_comm",
            "total_reward",
            "total_win_loss",
        ]

        for row in rows:

            if not isinstance(row, dict):
                continue

            doc = {}
            for field in ordered_fields:
                value = row.get(field, "")
                if value is None:
                    value = ""
                doc[field] = value

            username = row.get("username") or row.get("memberLogin")
            member_id = row.get("member_id") or row.get("player_id")

            if username:
                doc["username"] = str(username)
            if member_id:
                doc["member_id"] = str(member_id)

            cleaned_docs.append(doc.copy())

            # === Upsert by member_id (replace existing document when member_id matches) ===
            key_value = doc.get("member_id")

            # If we have a member_id, use it as the unique key and replace existing doc
            if key_value:
                try:
                    result = collection.replace_one(
                        {"member_id": key_value},
                        doc,
                        upsert=True
                    )
                    # New insert vs update existing
                    if result.upserted_id:
                        inserted += 1
                    else:
                        # treated as "skipped" in the sense of "updated existing"
                        skipped += 1
                except Exception:
                    # any failure to write is treated as skipped
                    skipped += 1
            else:
                # fallback: no member_id, behave like a simple insert
                try:
                    collection.insert_one(doc)
                    inserted += 1
                except Exception:
                    skipped += 1

        return cleaned_docs, inserted, skipped

    # Update Data to Google Sheet from MongoDB (All Member Report) IBS
    @classmethod
    def upload_to_google_sheet_AMR(cls, collection, gs_id, gs_tab, rows=None):

        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        SPREADSHEET_ID = gs_id
        RANGE_NAME = cls._build_a1_range(gs_tab, "A", 2, "AM")

        ordered_fields = [
            "none",
            "currency",
            "member_group",
            "username",
            "fullname",
            "member_id",
            "phone_no",
            "email",
            "vip_level",
            "aff_username",
            "member_tag",
            "deposit_count",
            "affiliate_transfer_in_count",
            "affiliate_transfer_in_amount",
            "total_deposit",
            "processing_fee",
            "received_deposit",
            "withdraw_count",
            "affiliate_transfer_out_count",
            "affiliate_transfer_out_amount",
            "total_withdraw",
            "adjust_count",
            "adjust_in",
            "adjust_out",
            "total_adjust",
            "bet_count",
            "total_valid_bet",
            "total_bonus_bet",
            "total_turnover",
            "total_valid_bonus_bet",
            "win_loss",
            "player_bonus_winloss",
            "total_bonus",
            "total_cancel_bonus",
            "total_rebate",
            "total_referral",
            "total_referral_comm",
            "total_reward",
            "total_win_loss",
        ]

        def sanitize_rows(raw_rows):
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    sanitized.append([
                        str(r.get(field, "")) if r.get(field, "") is not None else ""
                        for field in ordered_fields
                    ])
                else:
                    sanitized.append([""] * len(ordered_fields))
            return sanitized

        load_dotenv()
        MONGODB_URI = os.getenv("MONGODB_API_KEY")
        if not MONGODB_URI:
            raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
        client = MongoClient(MONGODB_URI)
        db = client["RETENTION_0226"]

        if not rows:
            collection_ref = db[collection]
            documents = list(collection_ref.find({}, {"_id": 0}).sort("username", 1))
            rows = documents

        rows = sanitize_rows(rows)
        row_count = len(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        body = {"values": rows, "majorDimension": "ROWS"}

        print(f"Uploading {len(rows)} rows to Google Sheet range {RANGE_NAME}")

        try:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME,
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.\n")

    # Upload to Google Sheet (From JSON List) (All Member Report) SSBO
    @classmethod
    def upload_to_google_sheet_SSBO_AMR(cls, file_name, g_sheet_tab, g_sheet_ID):
        """Upload CSV data to Google Sheet starting from row 2 (overwrite existing cells)."""
        import os, pandas as pd
        from googleapiclient.discovery import build

        creds = cls.googleAPI()
        sheet = build("sheets", "v4", credentials=creds).spreadsheets()

        csv_path = f"/Users/nera_thomas/Desktop/Telemarketing/excel_file/{file_name}.csv"
        if not os.path.exists(csv_path):
            print(f"❌ File not found: {csv_path}")
            return

        df = pd.read_csv(csv_path, encoding="utf-8-sig").fillna("")
        values = df.values.tolist()   # skip header row on sheet

        sheet.values().update(
            spreadsheetId=g_sheet_ID,
            range=f"'{g_sheet_tab}'!A2",
            valueInputOption="USER_ENTERED",
            body={"values": values}
        ).execute()

        print(f"✅ Uploaded {len(df)} data rows to '{g_sheet_tab}' starting from row 2.")

# Fetch Data
class Fetch(Automation, BO_Account, mongodb_2_gs):
    
    # =========================== GET Cookies ===========================

    # Get SSBO Cookies incase Cookies expired
    @classmethod
    def _ssbo_get_cookies(cls):
        with sync_playwright() as p:  
            
            # Load Env
            load_dotenv("/Users/nera_thomas/Desktop/Telemarketing/.env")

            # Wait for Chrome CDP to be ready
            cls.wait_for_cdp_ready()

            # Connect to running Chrome
            browser = p.chromium.connect_over_cdp("http://localhost:9333")
            context = browser.contexts[0] if browser.contexts else browser.new_context()    

            # Open a new browser page
            page = context.pages[0] if context.pages else context.new_page()
            page.goto("https://aw8.premium-bo.com/#/member/member-info", wait_until="load", timeout=0)

            # if announment appear, then click close
            try:
                # Wait for "Member" appear
                expect(page.locator("//body/jhi-main/jhi-route/div[@class='en']/div[@id='left-navbar']/jhi-left-menu-main-component[@class='full']/div[@class='row']/div[@class='col col-12 col-sm-12 col-md-12 col-lg-12 col-xl-12']/div[@id='left-menu-body']/jhi-sub-left-menu-component/ul[@class='navbar-nav flex-direction-col']/li[4]/div[1]/div[1]/a[1]/ul[1]/li[2]")).to_be_visible(timeout=30000)
                # Check whether "Merchant credit balance is low" appear, else pass
                expect(page.locator("//div[normalize-space()='Merchant credit balance is low.']")).to_be_visible(timeout=1500)
                # Click checkbox
                page.locator("//div[@class='disable-low-merchant-credit-balance']//input[@type='checkbox']").click()
                time.sleep(1)
                # Click Close
                page.locator("//button[normalize-space()='Close']").click()
            except:
                pass

            # if is in Login Page, then Login, else Skip
            try:
                # Check whether "Sign In" appear, else pass
                expect(page.locator("//h5[normalize-space()='Sign In?']")).to_be_visible(timeout=2000)
                # Fill in Username
                page.locator("//input[@placeholder='Username:']").fill(cls.accounts["super_swan"]["acc_ID"])
                # Fill in Password
                page.locator("//input[@id='password-input']").fill(cls.accounts["super_swan"]["acc_PASS"])
                # Login 
                page.click("//jhi-form-shared-component[@ng-reflect-disabled='false']//button[@class='btn btn-primary btn-form btn-submit login-label-color'][normalize-space()='Login']", force=True)
                # Delay 2 second
                page.wait_for_timeout(2000)
                # 
            except:
                pass

            # if is in Login Page, then Login, else Skip
            try:
                # Check whether "Sign In" appear, else pass
                expect(page.locator("//body[1]/ngb-modal-window[14]/div[1]/div[1]/jhi-re-login[1]/div[2]/jhi-login-route[1]/div[1]/div[1]/div[2]/jhi-form-shared-component[1]/form[1]/div[1]/div[1]/h5[1]")).to_be_visible(timeout=3000)
                # Fill in Username
                page.locator("//body[1]/ngb-modal-window[14]/div[1]/div[1]/jhi-re-login[1]/div[2]/jhi-login-route[1]/div[1]/div[1]/div[2]/jhi-form-shared-component[1]/form[1]/div[1]/div[2]/div[2]/jhi-text-shared-component[1]/div[1]/div[1]/div[1]/input[1]").fill(cls.accounts["super_swan"]["acc_ID"])
                # Fill in Password
                page.locator("//body[1]/ngb-modal-window[14]/div[1]/div[1]/jhi-re-login[1]/div[2]/jhi-login-route[1]/div[1]/div[1]/div[2]/jhi-form-shared-component[1]/form[1]/div[1]/div[3]/div[2]/jhi-password-shared-component[1]/div[1]/div[1]/div[1]/input[1]").fill(cls.accounts["super_swan"]["acc_PASS"])
                # Login 
                page.click("//jhi-form-shared-component[@ng-reflect-disabled='false']//button[@class='btn btn-primary btn-form btn-submit login-label-color'][normalize-space()='Login']", force=True)
                # Delay 2 second
                page.wait_for_timeout(2000)
                # Click Member
                page.locator("//body/jhi-main/jhi-route/div[@class='en']/div[@id='left-navbar']/jhi-left-menu-main-component[@class='full']/div[@class='row']/div[@class='col col-12 col-sm-12 col-md-12 col-lg-12 col-xl-12']/div[@id='left-menu-body']/jhi-sub-left-menu-component/ul[@class='navbar-nav flex-direction-col']/li[4]/div[1]/div[1]/a[1]/ul[1]/li[2]").click()
                # Click Member Info
                page.locator("//a[@ng-reflect-router-link='/member/member-info']//li[@class='parent-nav-item'][normalize-space()='1.3. Member Info']").click()
            except:
                pass
            
            # Click All
            page.locator("//button[normalize-space()='All']").click()
            # Delay 1 second
            page.wait_for_timeout(1000)

            # Extract all cookies
            cookies = context.cookies()

            # # Convert to Json Format
            # cookies_json = json.dumps(cookies, indent=4, ensure_ascii=False)
            # print(cookies_json)

            # Extract all cookies starting with "_ga"
            ga_cookies = [c for c in cookies if c.get("name", "").startswith("_ga")]

            # Initialize parts list
            parts = []

            # ============== Get GA1 Cookies ===============

            for c in ga_cookies:
                name = c.get("name")
                value = c.get("value")
                if name == "_ga":  # main GA cookie
                    parts.append(value)
                else:
                    parts.append(f"{name}={value}")

            # Join everything with "; "
            combined_ga = "; ".join(parts)
            # print(f"✅ Combined GA cookies: {combined_ga}")

            # =============== Get Bearer Token =================

            bearer_token = ""
            try:
                keys = page.evaluate("() => Object.keys(localStorage)")
                for key in keys:
                    value = page.evaluate(f"() => window.localStorage.getItem('{key}')")
                    # Find "jwt":"xxxxx" inside JSON text
                    match = re.search(r'"jwt"\s*:\s*"([^"]+)"', value or "")
                    if match:
                        bearer_token = match.group(1)
                        # print(f"✅ Found Bearer Token:\nBearer {bearer_token}")
                        break
            except Exception as e:
                print(f"⚠️ Error extracting bearer token: {e}")

            # Save cookie and bearer token to JSON file
            data = {
                "user_cookie": combined_ga,
                "bearer_token": bearer_token
            }
            output_path = "/Users/nera_thomas/Desktop/Telemarketing/get_cookies/superswan.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

            print(f"✅ Get Super Swan Cookies + Bearer Token Successful ...")

            # Browser Quit
            browser.close()
            Automation.cleanup()

    # =========================== ALL MEMBER REPORT ===========================

    # BO All Member Report (Extract Data like Postman/API and save as json file)
    # (merchants name = Acewin8, Ivip9, UEABET)
    @classmethod
    def ssbo_allmemberReport(cls, merchant, currency, file_name, g_sheet_tab, g_sheet_ID):
        with sync_playwright() as p:  

            # Wait for Chrome CDP to be ready
            cls.wait_for_cdp_ready()

            # Connect to running Chrome
            browser = p.chromium.connect_over_cdp("http://localhost:9333")
            context = browser.contexts[0] if browser.contexts else browser.new_context()    

            # Open a new browser page
            page = context.pages[0] if context.pages else context.new_page()
            page.goto("https://aw8.premium-bo.com/", wait_until="load", timeout=0)

            try:
                # if announment appear, then click close
                try:
                    # Wait for "Member" appear
                    expect(page.locator("//body/jhi-main/jhi-route/div[@class='en']/div[@id='left-navbar']/jhi-left-menu-main-component[@class='full']/div[@class='row']/div[@class='col col-12 col-sm-12 col-md-12 col-lg-12 col-xl-12']/div[@id='left-menu-body']/jhi-sub-left-menu-component/ul[@class='navbar-nav flex-direction-col']/li[4]/div[1]/div[1]/a[1]/ul[1]/li[2]")).to_be_visible(timeout=3000)
                    # Check whether "Merchant credit balance is low" appear, else pass
                    expect(page.locator("//div[normalize-space()='Merchant credit balance is low.']")).to_be_visible(timeout=1500)
                    # Click checkbox
                    page.locator("//div[@class='disable-low-merchant-credit-balance']//input[@type='checkbox']").click()
                    time.sleep(1)
                    # Click Close
                    page.locator("//button[normalize-space()='Close']").click()
                except:
                    pass
                
                # if is in Login Page, then Login, else Skip
                try:
                    # Check whether "Sign In" appear, else pass
                    expect(page.locator("//h5[normalize-space()='Sign In?']")).to_be_visible(timeout=2000)
                    # Fill in Username
                    page.locator("//input[@placeholder='Username:']").fill(cls.accounts["super_swan2"]["acc_ID"])
                    # Fill in Password
                    page.locator("//input[@id='password-input']").fill(cls.accounts["super_swan2"]["acc_PASS"])
                    # Login 
                    page.click("//jhi-form-shared-component[@ng-reflect-disabled='false']//button[@class='btn btn-primary btn-form btn-submit login-label-color'][normalize-space()='Login']", force=True)
                    # Delay 2 second
                    page.wait_for_timeout(2000)
                except:
                    pass

                # if is in Login Page, then Login, else Skip
                try:
                    # Check whether "Sign In" appear, else pass
                    expect(page.locator("//body[1]/ngb-modal-window[3]/div[1]/div[1]/jhi-re-login[1]/div[2]/jhi-login-route[1]/div[1]/div[1]/div[2]/jhi-form-shared-component[1]/form[1]/div[1]/div[1]/h5[1]")).to_be_visible(timeout=2000)
                    # Fill in Username
                    page.locator("//body[1]/ngb-modal-window[3]/div[1]/div[1]/jhi-re-login[1]/div[2]/jhi-login-route[1]/div[1]/div[1]/div[2]/jhi-form-shared-component[1]/form[1]/div[1]/div[2]/div[2]/jhi-text-shared-component[1]/div[1]/div[1]/div[1]/input[1]").fill(cls.accounts["super_swan2"]["acc_ID"])
                    # Fill in Password
                    page.locator("//body[1]/ngb-modal-window[3]/div[1]/div[1]/jhi-re-login[1]/div[2]/jhi-login-route[1]/div[1]/div[1]/div[2]/jhi-form-shared-component[1]/form[1]/div[1]/div[3]/div[2]/jhi-password-shared-component[1]/div[1]/div[1]/div[1]/input[1]").fill(cls.accounts["super_swan2"]["acc_PASS"])
                    # Login 
                    page.click("//jhi-form-shared-component[@ng-reflect-disabled='false']//button[@class='btn btn-primary btn-form btn-submit login-label-color'][normalize-space()='Login']", force=True)
                    # Delay 2 second
                    page.wait_for_timeout(2000)
                except:
                    pass
                
                
                # Click Report
                page.locator("//body/jhi-main/jhi-route/div[@class='en']/div[@id='left-navbar']/jhi-left-menu-main-component[@class='full']/div[@class='row']/div[@class='col col-12 col-sm-12 col-md-12 col-lg-12 col-xl-12']/div[@id='left-menu-body']/jhi-sub-left-menu-component/ul[@class='navbar-nav flex-direction-col']/li[7]/div[1]/div[1]/a[1]/ul[1]/li[2]").click()
                # Click All Member Report
                page.locator("//a[@ng-reflect-router-link='/report/all-member-report']//li[@class='parent-nav-item'][normalize-space()='10.2. All Member Report']").click() 
                # Delay 5 seconds
                page.wait_for_timeout(5000)
                
                # if is in Login Page, then Login, else Skip
                try:
                    # Check whether "Sign In" appear, else pass
                    expect(page.locator("//h5[normalize-space()='Sign In?']")).to_be_visible(timeout=5000)
                    # Fill in Username
                    page.locator("//input[@placeholder='Username:']").fill(cls.accounts["super_swan2"]["acc_ID"])
                    # Fill in Password
                    page.locator("//input[@id='password-input']").fill(cls.accounts["super_swan2"]["acc_PASS"])
                    # Login 
                    page.click("//button[normalize-space()='Login']", force=True)
                    # Delay 2 second
                    page.wait_for_timeout(2000)
                    # Click Report
                    page.locator("//body/jhi-main/jhi-route/div[@class='en']/div[@id='left-navbar']/jhi-left-menu-main-component[@class='full']/div[@class='row']/div[@class='col col-12 col-sm-12 col-md-12 col-lg-12 col-xl-12']/div[@id='left-menu-body']/jhi-sub-left-menu-component/ul[@class='navbar-nav flex-direction-col']/li[7]/div[1]/div[1]/a[1]/ul[1]/li[2]").click()
                    # Click All Member Report
                    page.locator("//a[@ng-reflect-router-link='/report/all-member-report']//li[@class='parent-nav-item'][normalize-space()='10.2. All Member Report']").click()    
                except:
                    pass
                
                # Click Region dropdown
                page.locator("ng-select .ng-select-container").nth(0).click()
                page.locator(".ng-dropdown-panel .ng-option").filter(has_text=merchant).click()
                # Delay 1 second
                page.wait_for_timeout(1000)
                # Click Region dropdown
                page.locator("ng-select .ng-select-container").nth(1).click()
                page.locator(".ng-dropdown-panel .ng-option").filter(has_text=currency).click()
                # Delay 1 second
                page.wait_for_timeout(1000)
                # Button Click "Today"
                page.locator("//button[normalize-space()='Today']").click()
                # Delay 1 second
                page.wait_for_timeout(1000)
                # Button Click "Search"
                page.locator("//button[normalize-space()='Search']").click()
                # wait for the first row data appear 
                page.locator("tbody tr").first.wait_for(state="visible", timeout=10000)
                # Delay 3 seconds
                page.wait_for_timeout(3000)

                # Download .CSV Report File
                with page.expect_download(timeout=600000) as download_info:
                    page.locator("//button[normalize-space()='Export']").click(timeout=10000)   # ---> button click downnload csv file
                download = download_info.value

                # Save the file manually
                base_dir = "/Users/nera_thomas/Desktop/Telemarketing/excel_file"
                os.makedirs(base_dir, exist_ok=True)
                download_path = os.path.join(base_dir, f"{file_name}.zip")
                download.save_as(download_path)

                # Unzip file
                cls.unzip(file_name)
                # Delay 1 second
                page.wait_for_timeout(1000)
                # Upload to Google Sheet
                cls.upload_to_google_sheet_SSBO_AMR(file_name, g_sheet_tab, g_sheet_ID)
            finally:
                try:
                    browser.close()
                except:
                    pass  # Browser already closed or failed to close
                Automation.cleanup()
                # Run Chrome Browser
                Automation.chrome_CDP()
                
###############=================================== CODE RUN HERE =======================================############

### ==== README YO!!!! ==== ####
# member_info format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name)
# member_info_2 format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name)
# deposit_list format = (bo link, bo name, currency, gmt time, MongoDB Collection, GS ID, GS Tab Name, google sheet start column, google sheet end column)


# ================== AUTO STOP DATE CONFIG ==================

gmt8 = pytz.timezone("Asia/Singapore")
now = datetime.now(gmt8)

# Calculate next month (safe for December)
if now.month == 12:
    stop_year = now.year + 1
    stop_month = 1
else:
    stop_year = now.year
    stop_month = now.month + 1

# Stop at 1st day of next month, 00:00 GMT+8
STOP_DATETIME = gmt8.localize(datetime(stop_year, stop_month, 1, 0, 0, 0))
print(f"Bot will stop automatically at: {STOP_DATETIME}")

# ================== AUTO STOP DATE CONFIG ==================

while True:
    try:
        
        # Auto-stop check
        current_time = datetime.now(gmt8)
        if current_time >= STOP_DATETIME:
            logger.info(f"Reached stop date ({STOP_DATETIME}). Bot is stopping.")
            break

        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-
        # ============================================== SSBO USING EXPORT METHOD ====================================================================================
        # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-
        
        # Run Chrome Browser
        Automation.chrome_CDP()

        print("\n\033[1;36mSSBO A8T THAI\033[0m \033[2m(VIEW, TIP, KUNG)\033[0m \033[1;36mTODAY AMR\033[0m")
        safe_call(Fetch.ssbo_allmemberReport, "Acewin8", "Thailand", "SSBO_A8T_TODAY_AMR", "TODAY AMR", "1sJkHxS9PUUrNAjxPwcsb5FVvQgAS6o1LiqG2gOeWJIg")

        # Close Browser
        Automation.cleanup()
    
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
        break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
