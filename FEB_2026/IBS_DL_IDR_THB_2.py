import os
import sys
import time
import json
import pytz
import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from colorama import Fore, Style
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from runtime import logger, safe_call, MONGODB_URI

PROXIES = {
    "http":  "http://127.0.0.1:10809",
    "https": "http://127.0.0.1:10809",
}

def create_session():
    s = requests.Session()
    s.proxies.update(PROXIES)
    s.trust_env = False   # VERY IMPORTANT
    return s

# BO Account
class BO_Account:

   # Load the .env file
   # Format to use in other Class ===>             cls.accounts["gc99"]["merchant_code"]
    load_dotenv(dotenv_path="/home/thomas/.env")

    accounts = {
        "super_swan": {
            "acc_ID": os.getenv("ACC_ID_SUPERSWAN"),
            "acc_PASS": os.getenv("ACC_PASS_SUPERSWAN")
        },
        "wdb1": {
            "acc_ID": os.getenv("ACC_ID_WDB1"),
            "acc_PASS": os.getenv("ACC_PASS_WDB1")
        },
        "22f": {
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
        "22w": {
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
        "nex8": {
            "merchant_code": os.getenv("MERCHANT_CODE_NEX8"),
            "acc_ID": os.getenv("ACC_ID_NEX8"),
            "acc_PASS": os.getenv("ACC_PASS_NEX8")
        },
        "828": {
            "merchant_code": os.getenv("MERCHANT_CODE_828"),
            "acc_ID": os.getenv("ACC_ID_828"),
            "acc_PASS": os.getenv("ACC_PASS_828")
        },
        "s191": {
            "merchant_code": os.getenv("MERCHANT_CODE_S191"),
            "acc_ID": os.getenv("ACC_ID_S191"),
            "acc_PASS": os.getenv("ACC_PASS_S191")
        },
        "gojudi": {
            "merchant_code": os.getenv("MERCHANT_CODE_GOJUDI"),
            "acc_ID": os.getenv("ACC_ID_GOJUDI"),
            "acc_PASS": os.getenv("ACC_PASS_GOJUDI")
        },
    }

# MongoDB <-> Google Sheet
class mongodb_2_gs:

    # Handles Google Sheets operations for the Telemarketing project.
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    TOKEN_PATH = "/home/thomas/api/google4/token.json"
    CREDS_PATH = "/home/thomas/api/google4/credentials.json"

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
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-= DEPOSIT LIST (PLAYER ID) =-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================

    # MongoDB Database
    def mongodbAPI_DL_PID(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["CONVERSION_0226"]
        collection = db[collection]

        # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("player_id", 1), ("amount", 1), ("completed_at", 1)],
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
            player_id = row.get("player_id")
            amount = row.get("amount")
            completed_at = row.get("completed_at")

            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS)
            dt = datetime.fromisoformat(completed_at)
            completed_at_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Build the new cleaned document (Use for upload data to MongoDB)
            doc = {
                "player_id": player_id,
                "amount": amount,
                "completed_at": completed_at_fmt,
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

        print(f"MongoDB Summary → Inserted: {inserted}, Skipped: {skipped}\n")
        return cleaned_docs

    # Update Data to Google Sheet from MongoDB
    @classmethod
    def upload_to_google_sheet_DL_PID(cls, collection, gs_id, gs_tab, start_column, end_column, rows=None):

        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = f"{gs_tab}!{start_column}3:{end_column}"

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Normalize rows into list-of-lists; keep completed_at unchanged."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    pid = str(r.get("player_id", ""))
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"  # force Google Sheets to treat as text
                    sanitized.append([
                        pid,
                        str(r.get("amount", "")),
                        r.get("completed_at", ""),  # do not coerce to str
                    ])
                elif isinstance(r, (list, tuple)):
                    pid = str(r[0]) if len(r) > 0 else ""
                    if pid and not pid.startswith("'"):
                        pid = f"'{pid}"
                    sanitized.append([
                        pid,
                        str(r[1]) if len(r) > 1 else "",
                        r[2] if len(r) > 2 else "",  # do not coerce to str
                    ])
                else:
                    sanitized.append([str(r), "", ""])
            return sanitized

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            client = MongoClient(MONGODB_URI)
            db = client["CONVERSION_0226"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("completed_at", 1))
            rows = documents
            
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload to Google Sheet.")
            return

        body = {"values": rows, "majorDimension": "ROWS"}

        print(f"Uploading {len(rows)} rows to Google Sheet range {RANGE_NAME}")

        try:
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME,
                valueInputOption="USER_ENTERED",  # let Sheets parse dates/numbers
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        # print("Rows to upload:", rows)
        print("Uploaded MongoDB data to Google Sheet.")

# Fetch Data
class Fetch(BO_Account, mongodb_2_gs):
    
    # =========================== GET Cookies ===========================

    # Get IBS Cookies incase Cookies expired
    @classmethod
    def _get_cookies(cls, bo_link, merchant_code, acc_id, acc_pass, cookies_path):
        
        session = create_session()

        url = f"https://v3-bo.{bo_link}/api/be/auth/loginV2"

        payload = json.dumps({
        "mer_code": merchant_code,
        "username": acc_id,
        "password": acc_pass,
        })
        
        headers = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'domain': f'v3-bo.{bo_link}',
        'gmt': '+08:00',
        'lang': 'en-US',
        'loggedin': 'false',
        'origin': f'https://v3-bo.{bo_link}',
        'page': '/en-us',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://v3-bo.{bo_link}/en-us',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-arch': '"arm"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"143.0.7499.147"',
        'sec-ch-ua-full-version-list': '"Google Chrome";v="143.0.7499.147", "Chromium";v="143.0.7499.147", "Not A(Brand";v="24.0.0.0"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"macOS"',
        'sec-ch-ua-platform-version': '"15.5.0"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'type': 'CUSTOM',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }

        # Get session
        session.get(f"https://v3-bo.{bo_link}/en-us", headers=headers)

        # Return Response
        response = session.post(url, headers=headers, data=payload)
        print(response.json())

        # to get Authentication Cookies
        user_cookie = session.cookies.get("user")

        # Save cookies to "get_cookies" path
        file_path = cookies_path

        # data format
        data = {
            "user_cookie": user_cookie
        }

        # write to json file
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

        print("Saved user cookie to:", file_path)

    # =========================== DEPOSIT LIST ===========================

    # Deposit List (Player ID)
    @classmethod
    def deposit_list_PID(cls, bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column):
        
        session = create_session()

        # Print Color Messages
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN} {team} {Fore.YELLOW} DEPOSIT LIST Data...{Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        # Get TimeZone (GMT+7)
        gmt7 = pytz.timezone("Asia/Bangkok")
        now_gmt7 = datetime.now(gmt7)

        # Get Current Time (GMT +7)    
        current_time = now_gmt7.time()
        print(current_time, "GMT+7")
        
        # Today & Yesterday Date
        today = now_gmt7.strftime("%Y-%m-%d")
        yesterday = (now_gmt7 - timedelta(days=1)).strftime("%Y-%m-%d")

        # Rule:
        # 00:00 - 00:14 → use yesterday
        # 00:15 onward → use today
        if current_time < datetime.strptime("01:00", "%H:%M").time():
            start_date = yesterday
            end_date = yesterday
        else:
            start_date = today
            end_date = today

        # Cookie File
        cookie_file = f"/home/thomas/get_cookies/{bo_link}.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"user_cookie": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            user_cookie = json.load(f).get("user_cookie", "")
        
        url = f"https://v3-bo.{bo_link}/api/be/finance/get-deposit"

        payload = {
        "paginate": 100,
        "page": 1,
        "currency": [
            currency
        ],
        "status": "approved",
        "start_date": start_date,
        "end_date": end_date,
        "gmt": gmt_time,
        "merchant_id": 1,
        "admin_id": 337,
        "aid": 337
        }
        headers = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'domain': f'v3-bo.{bo_link}',
        'gmt': gmt_time,
        'lang': 'en-US',
        'loggedin': 'true',
        'origin': f'https://v3-bo.{bo_link}',
        'page': '/en-us/finance-management/deposit',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://v3-bo.{bo_link}/en-us/finance-management/deposit',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'type': 'POST',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'Cookie': f"i18n_redirected=en-us; user={user_cookie}",
        }

        # Post Response 
        response = session.post(url, headers=headers, json=payload, timeout=30)

        # Check if return unauthorized (401) 
        if response.json().get("statusCode") == 401:
            
            # Print 401 error
            print("⚠️ Received 401 Unauthorized. Attempting to refresh cookies...")

            # Get Cookies
            cls._get_cookies(
                bo_link,
                cls.accounts[bo_name]["merchant_code"],
                cls.accounts[bo_name]["acc_ID"],
                cls.accounts[bo_name]["acc_PASS"],
                f"/home/thomas/get_cookies/{bo_link}.json"
            )
            
            # Retry request...
            return cls.deposit_list_PID(bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab, start_column, end_column)


        # For loop page and fetch data
        for page in range(1, 10000):
            payload["page"] = page

            # Send POST request
            response = session.post(url, headers=headers, json=payload, timeout=30)

            # Safe JSON Handling
            try:
                data = response.json()
            except Exception:
                print("Invalid JSON response from API!")
                print("Status Code:", response.status_code)
                print("Response text:", response.text[:500])
                return

            rows = data.get("data", [])
            print(f"\nPage {page} → {len(rows)} rows")

            # ====================== EMPTY PAGE SAFETY RETRY ======================
            # ⚠️ IMPORTANT:
            # This API is unstable and may randomly return 0 rows even when data exists.
            # A single (or even double) empty response MUST NOT be trusted.
            #
            # Strategy:
            # - Retry the SAME page up to MAX_EMPTY_RETRIES times
            # - Sleep briefly between retries to avoid backend race conditions
            # - If ANY retry returns data → treat page as valid
            # - Only stop pagination if ALL retries return 0 rows
            
            if not rows:
                MAX_EMPTY_RETRIES = 5  # Set to 5 retries
                empty_attempts = 0
                
                print(f"⚠️ Page {page} returned 0 rows — Starting safety retries...")

                while empty_attempts < MAX_EMPTY_RETRIES:
                    # Incrementing attempt number for display (1 to 8)
                    current_attempt = empty_attempts + 1
                    
                    # Print this BEFORE the request so you see it immediately
                    print(f"🔄 [Retry {current_attempt}/{MAX_EMPTY_RETRIES}] Re-requesting Page {page}...")
                    
                    time.sleep(1.5)  

                    try:
                        # Use session.post to ensure proxies/headers are maintained
                        retry_response = session.post(url, headers=headers, json=payload, timeout=30)
                        
                        if retry_response.status_code == 401:
                            print(f"❌ Attempt {current_attempt}: Unauthorized (401). Stopping retries.")
                            break

                        retry_data = retry_response.json()
                        rows = retry_data.get("data", []) 
                        
                        if rows:
                            print(f"✅ Success! Page {page} recovered on attempt {current_attempt} ({len(rows)} rows found).")
                            break
                        else:
                            print(f"Empty: Attempt {current_attempt} still returned 0 rows.")
                            
                    except Exception as e:
                        # This catches connection timeouts or proxy errors
                        print(f"⚠️ Attempt {current_attempt} failed with error: {e}")
                        rows = []

                    empty_attempts += 1

                if not rows:
                    print(f"⛔ Page {page} confirmed empty after {MAX_EMPTY_RETRIES} retries. Stopping pagination.")
                    break

            # Insert into MongoDB
            rows_to_insert = rows if isinstance(rows, list) else []

            if rows_to_insert:
                cls.mongodbAPI_DL_PID(rows_to_insert, collection)
            else:
                print("No data returned from API.")

        # Upload Data to Google Sheet by reading from MongoDB
        cls.upload_to_google_sheet_DL_PID(collection, gs_id, gs_tab, start_column, end_column)

# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= CODE RUN HERE =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= 
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

### ==== 🥹🥹🥹 README 🥹🥹🥹 ==== ####
### ==== 🥹🥹🥹 README 🥹🥹🥹 ==== ####
### ==== 🥹🥹🥹 README 🥹🥹🥹 ==== ####

#### FORMAT ---NEW REGISTER & DEPOSIT LIST--- FORMAT ####
# safe_call(Fetch.member_info, "BO Link", "merchant code", "team name", "currency", "gmt time", "database name", "google sheet link", "google sheet tab name", description="can write any")
# safe_call(Fetch.deposit_list_PID, "BO Link", "merchant code", "team name", "currency", "gmt time", "database name", "google sheet link", "google sheet tab name", "A", "C", description="can write any")

# ================== AUTO STOP DATE CONFIG ==================

gmt7 = pytz.timezone("Asia/Bangkok")
now = datetime.now(gmt7)

# Calculate 10th of next month
if now.month == 12:
    stop_year = now.year + 1
    stop_month = 1
else:
    stop_year = now.year
    stop_month = now.month + 1

# Stop at next month of 10th, 00:00 GMT+7
STOP_DATETIME = gmt7.localize(datetime(stop_year, stop_month, 10, 0, 0, 0))
print(f"Bot will stop automatically at: {STOP_DATETIME}")

# ================== AUTO STOP DATE CONFIG ==================

while True:
    try:

        # Auto-stop check
        current_time = datetime.now(gmt7)
        if current_time >= STOP_DATETIME:
            logger.info(f"Reached stop date ({STOP_DATETIME}). Bot is stopping.")
            break


        # ==========================================================================
        # =-=-=-=-==-=-=-=-= G855T DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-=-=-=-=-
        # ==========================================================================
        
        # MEME
        safe_call(Fetch.deposit_list_PID, "god855.asia", "g855", "G855T", "THB", "+07:00", "G855T_DL", "12AVH011V35qWUCqpMWfMpmYzhRflyaxWZ5BUCCYHWhQ", "DEPOSIT LIST", "A", "C", description="G855T deposit list")

        # VIEW, TIP, KUNG
        print("\n\033[1;36mG855T\033[0m \033[2m(VIEW, TIP, KUNG)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "G855T_DL", "1kJVDdpS3rAG9f31nk96HUloT1RJspu5oOSY-Txd7Wfs", "DEPOSIT LIST", "A", "C", description="G855T deposit list")

        # VIEW, TIP, KUNG
        print("\n\033[1;36mG855T\033[0m \033[2m(VIEW, TIP, KUNG)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "G855T_DL", "1fP0US23tZChapiQieKfRveFuG2glNUh15MPYJiC6PGQ", "Deposit List", "A", "C", description="G855T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= 2FT DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-= 
        # ==========================================================================

        # DAOW
        safe_call(Fetch.deposit_list_PID, "22funbo.com", "22f", "2FT", "THB", "+07:00", "2FT_DL", "1cizS7oiv6uD9DzmdiTVhIxQ8xgnSYNMgt93RtCsGI_E", "DEPOSIT LIST", "A", "C", description="2FT deposit list")

        # MUAY
        print("\n\033[1;36m2FT\033[0m \033[2m(MUAY)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "2FT_DL", "1qvdJ_YUmFSAcJ2ENRbanZpxCNBazYT_PP-R6jz8NVqE", "DEPOSIT LIST", "A", "C", description="2FT deposit list")

        # KAEO
        print("\n\033[1;36m2FT\033[0m \033[2m(KAEO)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "2FT_DL", "1cTUwdSANPnmtLOeUd1TG6MrcF1OnJ2y2TLvAeaJ8Cww", "Deposit List", "A", "C", description="2FT deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= M1T DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-= 
        # ==========================================================================

        # ALICE
        safe_call(Fetch.deposit_list_PID, "zupra7x.com", "mf191", "M1T", "THB", "+07:00", "M1T_DL", "1t_Nz_v0e6P-_rOlN9gyqSs2ncgZeGmbJsybk5M8ayDY", "DEPOSIT LIST", "A", "C", description="M1T deposit list")
        
        # KAEO
        print("\n\033[1;36mM1T\033[0m \033[2m(KAEO)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "M1T_DL", "1eQXzwqGE8uszuFvb19jwRq3zPYK5ojv3SCUzdQr2_vs", "DEPOSIT LIST", "A", "C", description="M1T deposit list")

        # KAEO
        print("\n\033[1;36mM1T\033[0m \033[2m(KAEO)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "M1T_DL", "1ccQ9N4kv9cyS9dBcmTw1jN_vY9pXBxq9RdFKmRVmp5I", "Deposit List", "A", "C", description="M1T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= 2WT DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-= 
        # ==========================================================================

        # NONG SI
        safe_call(Fetch.deposit_list_PID, "w8c4n9be.com", "22w", "2WT", "THB", "+07:00", "2WT_DL", "1TTd1TgPwZUX_sdQJX-CzdZM2imLZi1vLgUkBmFXgZQ8", "DEPOSIT LIST", "A", "C", description="2WT deposit list")
        
        # SAI
        print("\n\033[1;36m2WT\033[0m \033[2m(SAI)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "2WT_DL", "1JgEWHohCHU-SYtY4IMVnuTTfMIYartGEuXSllLawmto", "DEPOSIT LIST", "A", "C", description="2WT deposit list")

        # SATANG
        print("\n\033[1;36m2WT\033[0m \033[2m(SATANG)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "2WT_DL", "1H3te4ymNjPqt_yLyWbFz6HvdFWRIQP-AUYLsEqhT6_8", "Deposit List", "A", "C", description="2WT deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S8T DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-= 
        # ==========================================================================

        # MEW
        safe_call(Fetch.deposit_list_PID, "siam855bo.com", "s855", "S8T", "THB", "+07:00", "S8T_DL", "1BlvW1IwKMMHGN4n4kmh9nWZCoXHSdnn_Vw_6BT4k0Yk", "DEPOSIT LIST", "A", "C", description="S8T deposit list")

        # NINEW
        print("\n\033[1;36mS8T\033[0m \033[2m(NINEW)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "S8T_DL", "16t_9zYJRynTgkzQa0AUGVsjNGIv7YSIBrOWXMXpyaOc", "DEPOSIT LIST", "A", "C", description="S8T deposit list")
        
        # NINEW
        print("\n\033[1;36mS8T\033[0m \033[2m(NINEW)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "S8T_DL", "1MOCaMD48nxSyBMVG0O01kKL4c5KxGJz71gb5nTpVvxo", "Deposit List", "A", "C", description="S8T deposit list")
        
        # ==========================================================================
        # =-=-=-=-==-=-=-=-= N789T DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=
        # ==========================================================================

        # SOFIA
        safe_call(Fetch.deposit_list_PID, "q2n5w3z.com", "n789", "N789T", "THB", "+07:00", "N789T_DL", "15jh_5Aulx-PFwA73UhUg32iLon_kiLWw3reik-0t0uU", "DEPOSIT LIST", "A", "C")

        # TOKIE
        print("\n\033[1;36mN789T\033[0m \033[2m(TOKIE)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "N789T_DL", "1cnz2tnEMEh-Qnz_Viw-qJOpkVdV-oMrFmbY5EZjJP8U", "DEPOSIT LIST", "A", "C")

        # SUPATTA
        print("\n\033[1;36mN789T\033[0m \033[2m(SUPATTA)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "N789T_DL", "1H1PKac3Hd9dcIS-dTLBFMDKsCQBYUzX6PG9YCJ7hu_Y", "Deposit List", "A", "C")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= N1T DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-= 
        # ==========================================================================

        # TANGMO
        safe_call(Fetch.deposit_list_PID, "m8b4x1z6.com", "n191", "N1T", "THB", "+07:00", "N1T_DL", "1wIaHLvmXPypSpW5IWpwt8ex-ESwTgD0VGmugwqPz9tw", "DEPOSIT LIST", "A", "C")

        # SUPATTA
        print("\n\033[1;36mN1T\033[0m \033[2m(SUPATTA)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "N1T_DL", "1tlFaPMptGcwFEp1dtzBqSTz5R8ofWhOcasYi5QBphCI", "Deposit List", "A", "C")


       # RILLEY
        print("\n\033[1;36mN1T\033[0m \033[2m(JIRAPORN)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "N1T_DL", "1MN69M3PuBOHBDbLKk9Ycx5Z143PQeITDOE4xoLPHuMI", "DEPOSIT LIST", "A", "C")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= N855T DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-= 
        # ==========================================================================

        # ELSA
        print("\n\033[1;36mN855T\033[0m \033[2m(ELSA)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(Fetch.deposit_list_PID, "f5x3n8v.com", "n855", "N855T", "THB", "+07:00", "N855T_DL", "1PhrRokHUYNLKvI8QWINUD5DtbhtJNkkToDZB0k4frjQ", "DEPOSIT LIST", "A", "C")

        # MO
        print("\n\033[1;36mN855T\033[0m \033[2m(MO)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "N855T_DL", "1_k891xn4TYgpE6GkY1pyS4nQkoPRJt2f6dH2Enwg_oU", "DEPOSIT LIST", "A", "C")

        # JIRAPORN
        print("\n\033[1;36mN855T\033[0m \033[2m(JIRAPORN)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "N855T_DL", "1f1k9D-IKdL8qyoQyLLBPi_ww-cTRJV4-S1B3B4lkrKM", "Deposit List", "A", "C")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= GT DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-= 
        # ==========================================================================

        # NAREM
        safe_call(Fetch.deposit_list_PID, "gcwin99bo.com", "gc99", "GT", "THB", "+07:00", "GT_DL", "1X3_U_OaqDp7IYEVITUh-PaLJtq3VbV6MzI58wRzXcLs", "DEPOSIT LIST", "A", "C")

        # JIRAPORN
        print("\n\033[1;36mGT\033[0m \033[2m(JIRAPORN)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "GT_DL", "11tMkPwf4ZrvIpaLhfVBsEsX6B0IqQ03lVU8-eWF-v9o", "DEPOSIT LIST", "A", "C")

        # KOI
        print("\n\033[1;36mGT\033[0m \033[2m(KOI)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "GT_DL", "1lFrK5k8oRIW1e4aFtaLTB4dGtLnVgtwtlfMKQdtFqrc", "Deposit List", "A", "C")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= R99T DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-= 
        # ==========================================================================

        # POP
        safe_call(Fetch.deposit_list_PID, "bex7tor.com", "r99", "R99T", "THB", "+07:00", "R99T_DL", "19C_KqDqeqBNH_A1xM_GswP_UJ-Ei9d9UEvjb8xHUoeA", "DEPOSIT LIST", "A", "C")
        
        # VIEW, TIP, KUNG
        print("\n\033[1;36mR99T\033[0m \033[2m(VIEW, TIP, KUNG)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "R99T_DL", "1jvmDXr7KL0HJN8W3APDB69UNkCX_wAD75Pql4IkYA74", "DEPOSIT LIST", "A", "C")

        # VIEW, TIP, KUNG
        print("\n\033[1;36mR99T\033[0m \033[2m(VIEW, TIP, KUNG)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "R99T_DL", "1RtAI_Z3qPe3O_cw7AnP9x0geUuzlEd7Kr0primEolRU", "Deposit List", "A", "C")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S1T DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-=-=-=-=-=-=-=-=-=-  
        # ==========================================================================

        # KHAM
        safe_call(Fetch.deposit_list_PID, "sol9ven.com", "s191", "S1T", "THB", "+07:00", "S1T_DL", "1xPcWEPEyl0-i6nlUTDl0GIHQCRLlwSGZzY7H5If56Pw", "DEPOSIT LIST", "A", "C")

        # FON
        print("\n\033[1;36mS1T\033[0m \033[2m(FON)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "S1T_DL", "1EPG7UkOyqw6RodUjPPGZ1gSb0DS-xcjyKSYd27aOFEo", "DEPOSIT LIST", "A", "C") 

        # PHOUNG
        print("\n\033[1;36mS1T\033[0m \033[2m(PHOUNG)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "S1T_DL", "1pXwjY5mU0VOH4_X0PuF4RlDN8WLVZEmflkzjgp0-I_s", "DEPOSIT LIST", "A", "C") 

        # PHOUNG
        print("\n\033[1;36mS1T\033[0m \033[2m(PHOUNG)\033[0m\033[1;36m DL PID\033[0m")
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "S1T_DL", "1LCOw9dCu7CW-acsIZkCEzP_DvemZEC7EeSRuZn0n2vY", "Deposit List", "A", "C") 

        # Delay 5 seconds
        time.sleep(300)

    except KeyboardInterrupt:
            logger.info("Execution interrupted by user.")
            break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
