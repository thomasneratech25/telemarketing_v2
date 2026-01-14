import os
import sys
import time
import atexit
import logging
import requests
import subprocess
from pathlib import Path
from dotenv import load_dotenv
from colorama import Fore, Style
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
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
        USER_DATA_DIR = f"/Users/nera_thomas/Library/Application Support/Google/Chrome/Profile 9"
        
        # Start Chrome normally
        cls.chrome_proc = subprocess.Popen([
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "--remote-debugging-port=9222",
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
        """Wait until Chrome CDP is ready at http://localhost:9222/json"""
        for _ in range(timeout):
            try:
                res = requests.get("http://localhost:9222/json")
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

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO AMR =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

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

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-= SSBO DEPOSIT LIST (PLAYER ID) =-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

    # MongoDB Database
    def mongodbAPI_ssbo_DL_PID(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["J8MS_A8MS"]
        collection = db[collection]

        # Set and Ensure when upload data this 3 Field are Unique Data
        collection.create_index(
            [("memberLogin", 1), ("confirmedAmount", 1), ("lastModifiedDate", 1)],
            unique=True
        )

        # Count insert and skip
        inserted = 0
        skipped = 0
        cleaned_docs = []

        batch = []

        # for each rows in a list of JSON objects return
        for row in rows:
            # Extract only the fields you want (Extract Data from json file)
            memberLogin = row.get("memberLogin")
            confirmedAmount = row.get("confirmedAmount")
            lastModifiedDate = row.get("lastModifiedDate")

            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS) in GMT+8
            if lastModifiedDate:
                try:
                    # Convert Z → UTC datetime
                    dt = datetime.fromisoformat(lastModifiedDate.replace("Z", "+00:00"))

                    # Convert UTC → GMT+8 (Malaysia Time)
                    myt = dt.astimezone(timezone(timedelta(hours=8)))

                    # Format output
                    lastModifiedDate_fmt = myt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    lastModifiedDate_fmt = lastModifiedDate
            else:
                lastModifiedDate_fmt = ""

            # Build the new cleaned document (Use for upload data to MongoDB)
            doc = {
                "memberLogin": memberLogin,
                "confirmedAmount": confirmedAmount,
                "lastModifiedDate": lastModifiedDate_fmt
            }

            # Keep the version without _id for uploading later
            cleaned_docs.append(doc.copy())
            batch.append(doc)

            if len(batch) == 500:
                try:
                    collection.insert_many(batch, ordered=False)
                    inserted += len(batch)
                except Exception as exc:
                    if hasattr(exc, "details"):
                        skipped += len(exc.details.get("writeErrors", []))
                        inserted += len(batch) - len(exc.details.get("writeErrors", []))
                    else:
                        skipped += 0
                batch = []

        # Insert any remaining documents in batch
        if batch:
            try:
                collection.insert_many(batch, ordered=False)
                inserted += len(batch)
            except Exception as exc:
                if hasattr(exc, "details"):
                    skipped += len(exc.details.get("writeErrors", []))
                    inserted += len(batch) - len(exc.details.get("writeErrors", []))
                else:
                    skipped += 0

        print(f"MongoDB Summary → Inserted: {inserted}, Skipped: {skipped}")
        return cleaned_docs

    # Update Data to Google Sheet from MongoDB
    @classmethod
    def upload_to_google_sheet_ssbo_DL_PID(cls, collection, gs_id, gs_tab, start_column, end_column, rows=None):

        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id

        # Convert from MongoDB (dicts) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Normalize rows into list-of-lists; SSBO mapping."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    pid = str(r.get("memberLogin", ""))
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"  # force Google Sheets to treat as text
                    # --- BEGIN PATCHED AMOUNT HANDLING ---
                    amount_raw = str(r.get("confirmedAmount", ""))

                    # Force 2 decimal places WITHOUT rounding
                    if "." in amount_raw:
                        whole, frac = amount_raw.split(".", 1)
                        frac = (frac + "00")[:2]   # pad then truncate
                        amount_clean = f"{whole}.{frac}"
                    else:
                        amount_clean = f"{amount_raw}.00"

                    # --- END PATCHED AMOUNT HANDLING ---
                    sanitized.append([
                        pid,
                        amount_clean,
                        r.get("lastModifiedDate", ""),
                    ])
                elif isinstance(r, (list, tuple)):
                    pid = str(r[0]) if len(r) > 0 else ""
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"
                    sanitized.append([
                        pid,
                        str(r[1]) if len(r) > 1 else "",
                        r[2] if len(r) > 2 else "",
                    ])
                else:
                    sanitized.append([str(r), "", ""])
            return sanitized

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            load_dotenv()
            MONGODB_URI = os.getenv("MONGODB_API_KEY")
            if not MONGODB_URI:
                raise RuntimeError("MONGODB_API_KEY is not set. Please add it to your environment or .env file.")
            client = MongoClient(MONGODB_URI)

            db = client["J8MS_A8MS"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("lastModifiedDate", 1))
            rows = documents
            
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        first_empty_row = 4
        end_row = first_empty_row + len(rows) - 1
        target_range = cls._build_a1_range(gs_tab, start_column, first_empty_row, end_column, end_row)

        body = {"values": rows, "majorDimension": "ROWS"}

        cls._ensure_sheet_row_capacity(service, SPREADSHEET_ID, gs_tab, end_row)

        print(f"Uploading {len(rows)} rows to Google Sheet range {target_range}")

        try:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=target_range,
                valueInputOption="USER_ENTERED",  # let Sheets parse dates/numbers
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        if (start_column, end_column) in {("I", "K"), ("M", "O")}:
            cls._sort_range_by_column(
                service,
                SPREADSHEET_ID,
                gs_tab,
                start_column,
                end_column,
                start_row=first_empty_row,
                end_row=end_row,
                sort_column_letter=end_column,
                descending=False
            )

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.\n\n")

# Fetch Data
class Fetch(Automation, BO_Account, mongodb_2_gs):
    
    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO GET COOKIES =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================

    # Get SSBO Cookies incase Cookies expired
    @classmethod
    def _ssbo_get_cookies(cls, acc_id, acc_pass):

        session = requests.Session()

        url = "https://aw8.premium-bo.com/api/authenticate?noBlockUI=true"

        payload = json.dumps({
        "tenantCode": None,
        "login": acc_id,
        "password": acc_pass,
        "rememberMe": True,
        "authenticatorAppEncoded": None,
        "authenticatorAppCode": None
        })

        headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'origin': 'https://aw8.premium-bo.com',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://aw8.premium-bo.com/',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }

        # Session
        session.get("https://aw8.premium-bo.com/", headers=headers)

        # Response
        response = session.post(url, headers=headers, data=payload)
        data = response.json()
        jwt_token = data.get("jwt")

        # Save cookies to "get_cookies" path
        file_path = "/Users/nera_thomas/Desktop/Telemarketing/get_cookies/demo_cookies.json"

        # data format you want
        data = {"jwt_token": jwt_token}

        # write to json file
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print("Saved user cookie to:", file_path)

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO AMR =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=
    # ====================================================================================

    # (merchants name = Acewin8, Ivip9, UEABET)
    @classmethod
    def ssbo_allmemberReport(cls, merchant, currency, file_name, g_sheet_tab, g_sheet_ID):
        with sync_playwright() as p:  

            # Get Deposit List Data (effect for fun only)
            msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN}[{file_name}] {Fore.YELLOW}All Member Report...{Style.RESET_ALL}\n"
            for ch in msg:
                sys.stdout.write(ch)
                sys.stdout.flush()
                time.sleep(0.02)

            # Wait for Chrome CDP to be ready
            cls.wait_for_cdp_ready()

            # Connect to running Chrome
            browser = p.chromium.connect_over_cdp("http://localhost:9222")
            context = browser.contexts[0] if browser.contexts else browser.new_context()    

            # Open a new browser page
            page = context.pages[0] if context.pages else context.new_page()
            page.goto("https://aw8.premium-bo.com/", wait_until="load", timeout=0)

            try:
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
                    page.locator("//input[@placeholder='Username:']").fill(cls.accounts["super_swan"]["acc_ID"])
                    # Fill in Password
                    page.locator("//input[@id='password-input']").fill(cls.accounts["super_swan"]["acc_PASS"])
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
                # Button Click "This Month"
                page.locator("//button[normalize-space()='This Month']").click()
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

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- SSBO DEPOSIT LIST =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================

    # SSBO Deposit List (Player ID)
    @classmethod
    def ssbo_deposit_list_PID(cls, merchants, currency, collection, gs_id, gs_tab, start_column, end_column, upload_to_sheet=True):
        
        # Get current time in GMT+8
        gmt8 = pytz.timezone("Asia/Singapore")   # GMT+8
        now_gmt8 = datetime.now(gmt8)

        current_time = now_gmt8.time()
        print(current_time, "GMT+8")

        # Get today and yesterday date
        today = now_gmt8.strftime("%Y-%m-%d")
        yesterday = (now_gmt8 - timedelta(days=1)).strftime("%Y-%m-%d")

        # Rule:
        # 00:00 - 00:14 → yesterday
        # 00:15 onward → today
        cutoff_time = datetime.strptime("01:00", "%H:%M").time()

        if current_time < cutoff_time:
            start_date = yesterday
            end_date = yesterday
        else:
            start_date = today
            end_date = today

        # Cookie File
        cookie_file = f"/Users/nera_thomas/Desktop/Telemarketing/get_cookies/superswan.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"user_cookie": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            cookie_data = json.load(f)

        user_cookie = cookie_data.get("user_cookie", "")
        bearer_token = cookie_data.get("bearer_token", "")

        # ======================================= PART 1: Get the ID ==========================================
        
        ids = []
        page = 0
        
        while True:

            url = "https://aw8.premium-bo.com/cashmarket/api/sbo/deposit-withdrawal-management/transactions-deposit"

            params = {
                "page": page,
                "size": 2000,
                "sort": ["membershipLevel,DESC", "createdDate,DESC"],
                "tenantId": 35,
                "merchants": merchants,
                "merchant": merchants,
                "merchantCode": merchants,
                "currencies": currency,
                "start": f"{end_date}T16:00:00.000Z",
                "startTime": f"{end_date}T16:00:00.000Z",
                "startCreatedTime": f"{end_date}T16:00:00.000Z",
                "end": f"{today}T15:59:59.000Z",
                "endTime": f"{today}T15:59:59.000Z",
                "endCreatedTime": f"{today}T15:59:59.000Z",
                "transType": "D",
                "approved": "true",
                "rejected": "false",
                "pending": "false",
                "inProgress": "false",
                "risk": "false",
                "kyc": "false",
                "isProcessingTime": "false",
                "hasDepositHideProcessingFees": "false",
                "isAllowViewAccountNumber": "false",
                "maskReloadAccountNumberCurrencyList": "",
                "firstDeposit": "ALL",
                "hiddenColumns": [
                    "merchant",
                    "region",
                    "affiliateGroupCategory",
                    "affiliateLogin",
                    "processingFee",
                    "bankStatus"
                ],
                "isSeamlessWalletMerchant": "null",
                "cacheBuster": str(int(time.time() * 1000))
            }


            headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': f'Bearer {bearer_token}',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://aw8.premium-bo.com/',
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'Cookie': f'_ga={user_cookie}',
            }

            # Get Post Response
            query = urlencode(params, doseq=True)
            full_url = f"{url}?{query}"

            response = requests.get(full_url, headers=headers)

            # Handle auth errors before trying to parse JSON
            if response.status_code in (401, 403):
                print(f"⚠️ Received {response.status_code} from server. Attempting to refresh cookies + bearer token...")
                Automation.chrome_CDP()
                cls._ssbo_get_cookies()
                print("⚠️ Cookies + bearer token refreshed ... Retrying request...\n")
                cls.ssbo_deposit_list_PID(merchants, currency, collection, gs_id, gs_tab, start_column, end_column)
                return
            
            # Try to parse JSON safely
            try:
                data = response.json()
            except ValueError:
                print("⚠️ Response is not valid JSON.")
                print("Status code:", response.status_code)
                print("Text preview:", response.text[:500])
                return

            # List store all Member ID
            ids = []
            try:
                data = response.json()  # Try converting to JSON
                if isinstance(data, list):
                    ids = [item.get("id") for item in data if isinstance(item, dict) and "id" in item]
                    # print("All IDs:", ids)
                else:
                    print("⚠️ Unexpected JSON format:", data)
            except ValueError:
                print("⚠️ Response not valid JSON:")
                print(response.text)

            # print(f"✅ Page {page}: Total IDs found:", len(ids))
            
            # ======================================= PART 2: Use IDs to GET DATA  (funny lol) ==========================================
            
            if not ids:
                print("⚠️ No transaction IDs found — break\n")
                break

            base_url = "https://aw8.premium-bo.com/cashmarket/api/sbo/deposit-withdrawal-management/transactions-deposit-details"
            params = {
                "page": 0,
                "size": 2000,
                "sort": ["membershipLevel,ASC", "createdDate,ASC"],
                "tenantId": 35,
                "merchants": merchants,
                "merchant": merchants,
                "merchantCode": merchants,
                "currencies": currency,
                "start": f"{end_date}T16:00:00.000Z",
                "startTime": f"{end_date}T16:00:00.000Z",
                "startCreatedTime": f"{end_date}T16:00:00.000Z",
                "end": f"{today}T15:59:59.000Z",
                "endTime": f"{today}T15:59:59.000Z",
                "endCreatedTime": f"{today}T15:59:59.000Z",
                "transType": "D",
                "approved": "true",
                "rejected": "false",
                "pending": "false",
                "inProgress": "false",
                "risk": "false",
                "kyc": "false",
                "isProcessingTime": "false",
                "hasDepositHideProcessingFees": "false",
                "isAllowViewAccountNumber": "false",
                "maskReloadAccountNumberCurrencyList": "",
                "firstDeposit": "ALL",
                "hiddenColumns": [
                    "merchant",
                    "region",
                    "affiliateGroupCategory",
                    "affiliateLogin",
                    "processingFee",
                    "bankStatus"
                ],
                "isSeamlessWalletMerchant": "null",
                "cacheBuster": str(int(time.time() * 1000))
            }

            base_query = urlencode(params, doseq=True)
            transaction_ids_query = "transactionIds=" + "%2C".join(str(i) for i in ids)
            url = f"{base_url}?{base_query}&{transaction_ids_query}"

            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-US,en;q=0.9',
                'authorization': f'Bearer {bearer_token}',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'priority': 'u=1, i',
                'referer': 'https://aw8.premium-bo.com/',
                'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
                'Cookie': f'_ga={user_cookie}',
            }

            response = requests.get(url, headers=headers)
            # print(response.json())

            # Safe JSON Handling
            try:
                data = response.json()
            except Exception:
                print("Invalid JSON response from API!")
                print("Status Code:", response.status_code)
                print("Response text:", response.text[:500])
                return

            # Normalize data: SSBO sometimes returns a LIST, sometimes a DICT
            if isinstance(data, list):
                rows = data
            elif isinstance(data, dict):
                rows = data.get("data", [])
            else:
                print("⚠️ Unexpected response type:", type(data))
                return

            print(f"\nPage {page} → {len(rows)} rows")

            # STOP when no data
            if not rows:
                print(f"Finished. Last page = {page-1}")
                break

            # Insert into MongoDB
            if rows:
                cls.mongodbAPI_ssbo_DL_PID(rows, collection)
            else:
                print("No valid data returned from API.")

            page+=1

        if upload_to_sheet:
            cls.upload_to_google_sheet_ssbo_DL_PID(collection, gs_id, gs_tab, start_column, end_column)
        
###############=================================== CODE RUN HERE =======================================############


while True:
    try:

        # # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-
        # # ============================================== SSBO 9T USING EXPORT METHOD ====================================================================================
        # # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-
        
        safe_call(Fetch.ssbo_allmemberReport, "Ivip9", "Thailand", "SSBO_9T_AMR", "TM - All Member Report (SS)", "1xy2C52zKX0o6Odcc3TysmPHXuXXhdYIJJyi1NoRrDBw")

        safe_call(Fetch.ssbo_deposit_list_PID, "ip9", ["THB"], "SSBO_9T_DL", "1fL_qVhAKC8BmbPYrUlxSv4iT8CuFJk3zn4jv_xe8rjM", "DEPOSIT LIST", "A", "C", description="SSBO 9T DL PID")
        

        # # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-
        # # ============================================== SSBO A8MS USING EXPORT METHOD ====================================================================================
        # # =-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=-=-=-=-
        
        # Run Chrome Browser
        Automation.chrome_CDP()

        print(">>== SSBO A8M MY (ANGIE) ==<<")
        safe_call(Fetch.ssbo_allmemberReport, "Acewin8", "Malaysia", "SSBO_A8M_AMR", "TM - All Member Report (SS)", "1xy2C52zKX0o6Odcc3TysmPHXuXXhdYIJJyi1NoRrDBw")

        print(">>== SSBO A8M MY (ANGIE 2) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_SSBO_AMR, "SSBO_A8M_AMR", "TM - All Member Report (SS)", "1OtitCR8PXD9WXrOoXrluUdfMPZvzzzN__u4sulB9bio")

        print(">>== SSBO A8M MY (AVA) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_SSBO_AMR, "SSBO_A8M_AMR", "TM - All Member Report (SS)", "1bzFhQ6ji5Ch2sk-V1Cc3y2PNWk4CrSBllXhTN2deZX4")

        print(">>== SSBO A8S SG (AVA) ==<<")
        safe_call(Fetch.ssbo_allmemberReport, "Acewin8", "Singapore", "SSBO_A8S_AMR", "TM - All Member Report (SS)", "1V_qWbLfSJloA6KtEW7QdXz9qkox53WYRP46yRePgD90")

        print(">>== SSBO A8S SG (CINDY) ==<<")
        safe_call(mongodb_2_gs.upload_to_google_sheet_SSBO_AMR, "SSBO_A8S_AMR", "TM - All Member Report (SS)", "1JmVXGT67naNtM_9GiqPgDKqZXYdarKD-4nfy4zKTeMU")

        # Close Browser
        Automation.cleanup()

        # Delay 5 minutes
        time.sleep(300)

    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
        break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
