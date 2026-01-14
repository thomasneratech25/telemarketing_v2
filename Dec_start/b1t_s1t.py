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
        "b191": {
            "merchant_code": os.getenv("MERCHANT_CODE_B191"),
            "acc_ID": os.getenv("ACC_ID_B191"),
            "acc_PASS": os.getenv("ACC_PASS_B191")
        },
        "s191": {
            "merchant_code": os.getenv("MERCHANT_CODE_S191"),
            "acc_ID": os.getenv("ACC_ID_S191"),
            "acc_PASS": os.getenv("ACC_PASS_S191")
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
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-= DEPOSIT LIST (PLAYER ID) =-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================

    # MongoDB Database
    def mongodbAPI_DL_PID(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["CONVERSION"]
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
            db = client["CONVERSION"]
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

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- MEMBER INFO =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # ====================================================================================
    
    # MongoDB Database (Member Info)
    def mongodbAPI_MI(rows, collection):

        # MongoDB API KEY
        MONGODB_URI = os.getenv("MONGODB_API_KEY")

        client = MongoClient(MONGODB_URI)
        db = client["CONVERSION"]
        collection = db[collection]

        # Ensure unique member_id
        collection.create_index(
            [("member_id", 1)],
            unique=True
        )

        inserted = 0
        skipped = 0
        cleaned_docs = []
        batch = []

        for row in rows:

            # Safety check
            if not isinstance(row, dict):
                continue

            # ================= FIELD EXTRACTION =================
            username = row.get("username")
            first_name = row.get("first_name")
            mobileno = row.get("mobileno")
            member_id = row.get("member_id")
            email = row.get("email")

            # register_info parsing
            register_info_raw = row.get("register_info", "")
            register_info_date = None

            if register_info_raw:
                try:
                    register_info_date = register_info_raw.split("|")[0]
                    register_info_date = datetime.fromisoformat(
                        register_info_date.replace(" ", "T")
                    ).strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    register_info_date = None

            # Skip invalid rows (no primary key)
            if not member_id:
                continue

            # ================= BUILD DOCUMENT =================
            doc = {
                "username": username,
                "first_name": first_name,
                "register_info_date": register_info_date,
                "mobileno": mobileno,
                "member_id": member_id,
                "email": email,
            }

            cleaned_docs.append(doc.copy())
            batch.append(doc)

            # ================= BATCH INSERT =================
            if len(batch) == 500:
                try:
                    collection.insert_many(batch, ordered=False)
                    inserted += len(batch)
                except Exception as exc:
                    if hasattr(exc, "details"):
                        errors = exc.details.get("writeErrors", [])
                        skipped += len(errors)
                        inserted += len(batch) - len(errors)
                batch = []

        # ================= FINAL INSERT =================
        if batch:
            try:
                collection.insert_many(batch, ordered=False)
                inserted += len(batch)
            except Exception as exc:
                if hasattr(exc, "details"):
                    errors = exc.details.get("writeErrors", [])
                    skipped += len(errors)
                    inserted += len(batch) - len(errors)

        print(f"MongoDB Summary → Inserted: {inserted}, Skipped: {skipped}")
        return cleaned_docs

    # Update Data to Google Sheet from MongoDB (MemberInfo)
    @classmethod
    def upload_to_google_sheet_MI(cls, collection, gs_id, gs_tab, rows=None):
        
        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = f"{gs_tab}!A3:F"

        # Convert from MongoDB (dics) to Google Sheet API (list), because Google Sheets API only accept "list".
        def sanitize_rows(raw_rows):
            """Convert MongoDB docs to Google Sheet rows for Member Info."""
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    sanitized.append([
                        str(r.get("username", "")),
                        str(r.get("first_name", "")),
                        str(r.get("register_info_date", "")),
                        str(r.get("mobileno", "")),
                        str(r.get("member_id", "")),
                        str(r.get("email", "")),
                    ])
                else:
                    sanitized.append(["", "", "", "", "", ""])
            return sanitized

        # If no data upload to MongoDB, it auto upload data to google sheet
        if not rows:
            client = MongoClient(MONGODB_URI)
            db = client["CONVERSION"]
            collection = db[collection]
            documents = list(collection.find({}, {"_id": 0}).sort("register_info_date", 1))
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

    # Update Data to Google Sheet from MongoDB (MemberInfo) Split Data (Odd/Even)
    @classmethod
    def upload_to_google_sheet_MI_SPLIT_DATA(cls, collection, gs_ids, gs_tab, rows=None):
        """
        Split Member Info rows into odd/even:
        - odd rows → gs_ids[0]
        - even rows → gs_ids[1]
        """

        # Authenticate
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Convert MongoDB docs → Google Sheets rows
        def sanitize_rows(raw_rows):
            sanitized = []
            for r in raw_rows:
                if isinstance(r, dict):
                    sanitized.append([
                        str(r.get("username", "")),
                        str(r.get("first_name", "")),
                        str(r.get("register_info_date", "")),
                        str(r.get("mobileno", "")),
                        str(r.get("member_id", "")),
                        str(r.get("email", "")),
                    ])
                else:
                    sanitized.append(["", "", "", "", "", ""])
            return sanitized

        # Load from MongoDB if not provided
        if not rows:
            client = MongoClient(MONGODB_URI)
            db = client["CONVERSION"]
            col = db[collection]

            documents = list(col.find({}, {"_id": 0}).sort("register_info_date", 1))
            rows = documents

        # Format rows for GS API
        rows = sanitize_rows(rows)

        if not rows:
            print("No rows found to upload.")
            return

        # Split odd/even
        odd_rows = []
        even_rows = []

        for i, row in enumerate(rows):
            if (i + 1) % 2 == 0:
                even_rows.append(row)  # row #2,4,6,8...
            else:
                odd_rows.append(row)   # row #1,3,5,7...

        chunks = [odd_rows, even_rows]

        # Upload each split dataset to each Sheet ID
        for idx, chunk in enumerate(chunks):
            if idx >= len(gs_ids):
                break

            SPREADSHEET_ID = gs_ids[idx]
            RANGE_NAME = f"{gs_tab}!A3:F"

            print(f"Uploading {len(chunk)} rows → Sheet {idx+1} ({SPREADSHEET_ID})")

            body = {"values": chunk, "majorDimension": "ROWS"}

            # Upload
            sheet.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME,
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()

        print("All split uploads completed.")

# Fetch Data
class Fetch(BO_Account, mongodb_2_gs):
    
    # =========================== GET Cookies ===========================

    # Get IBS Cookies incase Cookies expired
    @classmethod
    def _get_cookies(cls, bo_link, merchant_code, acc_id, acc_pass, cookies_path):
        
        session = create_session()

        url = f"https://bo.{bo_link}/auth/login"

        payload = {
            "mer_code": merchant_code,
            "username": acc_id,
            "password": acc_pass,
        }

        headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': f'https://bo.{bo_link}',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://bo.{bo_link}/',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
        }


        # Initial request (sets CSRF / session cookies)
        session.get(f"https://bo.{bo_link}/en-us", headers=headers)

        # Login request
        response = session.post(url, headers=headers, data=payload, allow_redirects=True)

        # Optional: debug login result
        try:
            print("Login response:", response.json())
        except Exception:
            print("Login response not JSON")

        # Get cookies as dict
        cookies_dict = session.cookies.get_dict()

        # Build Cookie header string (EXACT format you want)
        cookie_string = "; ".join(
            f"{k}={v}" for k, v in cookies_dict.items()
        )

        # Print cookie string
        print(cookie_string)

        # Save cookies (dict form) to file
        with open(cookies_path, "w") as f:
            json.dump(cookies_dict, f, indent=2)

        # Return cookie string
        return cookie_string

    # Member Info
    @classmethod
    def member_info(cls, bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab, retry=False):
        
        # Print Team Name Messages
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN} {team} {Fore.YELLOW} MEMBER INFO Data...{Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        session = create_session()

        cookie_file = f"/home/thomas/get_cookies/{bo_link}.json"
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)

        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write("{}")

        with open(cookie_file, "r") as f:
            cookies = json.load(f)

        cookie_header = "; ".join(f"{k}={v}" for k, v in cookies.items())

        # ================= OUTPUT TXT FILE =================
        output_file = f"/home/thomas/member_info_{bo_link}.txt"
        txt = open(output_file, "a", encoding="utf-8")
        txt.write(f"\n\n===== START RUN {datetime.now()} =====\n")


        url = f"https://bo.{bo_link}/index/member-info-list"
        
        payload_template = (
            f"merchant_id=1&status=-1&id=&member_id=&username=&aff_code=&aff_username="
            f"&referral=&member_name=&currency=THB&account_number="
            f"&register_from=2026-01-01&register_to=2026-01-06"
            f"&mobile_no=&member_group=&kyc=&risk=&vip_level="
            f"&last_login_from=&last_login_to=&social_media=&userEmail="
            f"&referral_link=&last_deposit_from=&last_deposit_to="
            f"&page={{page}}&isExport=0"
        )

        headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': f'https://bo.{bo_link}',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://bo.{bo_link}/member-info-new',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"macOS"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
        'Cookie': cookie_header,
        }

    # ================= PAGINATION CONTROL =================
        # Track seen member IDs to avoid infinite loop
        seen_member_ids = set()

        # Hard safety limit (API bug protection)
        MAX_PAGES = 500

        for page in range(1, MAX_PAGES + 1):

            # Inject page number
            payload = payload_template.format(page=page)

            # Send request
            response = session.post(url, headers=headers, data=payload, timeout=30)

            # ================= SESSION VALIDATION =================
            content_type = response.headers.get("content-type", "")

            # If response is not JSON → session expired / redirected
            if "application/json" not in content_type.lower():
                print("⚠️ Session expired")

                # Retry ONCE by refreshing cookies
                if not retry:
                    print("🔁 Refreshing cookies...")
                    cls._get_cookies(
                        bo_link,
                        cls.accounts[bo_name]["merchant_code"],
                        cls.accounts[bo_name]["acc_ID"],
                        cls.accounts[bo_name]["acc_PASS"],
                        cookie_file,
                    )
                    txt.close()

                    # Restart function (retry=True prevents infinite retry)
                    return cls.member_info(
                        bo_link,
                        bo_name,
                        team,
                        currency,
                        gmt_time,
                        collection,
                        gs_id,
                        gs_tab,
                        retry=True,
                    )
                else:
                    print("❌ Already retried, stopping.")
                    break

            # ================= PARSE RESPONSE =================
            data = response.json()
            block = data.get("data", {})
            rows = block.get("item", [])

            # Defensive: ensure rows is a list
            if not isinstance(rows, list):
                rows = []

            # ================= DEDUPLICATION (WITH FULL DEBUG) =================
            new_rows = []
            duplicate_member_ids = []

            for row in rows:
                if not isinstance(row, dict):
                    continue

                member_id = row.get("member_id")
                if not member_id:
                    continue

                if member_id in seen_member_ids:
                    # 🔴 DEBUG: capture duplicate ID
                    duplicate_member_ids.append(member_id)
                    continue

                seen_member_ids.add(member_id)
                new_rows.append(row)

            # 🔍 DEBUG OUTPUT
            if duplicate_member_ids:
                print(
                    f"\n⚠️ DUPLICATE DETECTED on page {page}\n"
                    f"Total duplicates this page: {len(duplicate_member_ids)}\n"
                    f"Duplicate member_id list:\n{duplicate_member_ids}\n"
                )

            print(
                f"DEBUG Page {page} SUMMARY → "
                f"rows={len(rows)}, "
                f"new_rows={len(new_rows)}, "
                f"seen_total={len(seen_member_ids)}"
            )

            # ================= STOP CONDITION =================
            # If this page has NO new data → pagination ended
            if not new_rows:
                print(f"No new rows on page {page}, stopping pagination.")
                break

            # ================= SAVE DATA =================
            # Write only NEW rows to TXT
            for i, row in enumerate(new_rows, start=1):
                txt.write(
                    f"\nPage {page} | Row {i}\n"
                    f"{json.dumps(row, indent=2, ensure_ascii=False)}\n"
                )

            print(f"Page {page} → {len(new_rows)} new rows")

        # ================= CLEANUP =================
        txt.write(f"\n===== END RUN {datetime.now()} =====\n")
        txt.close()

        print(f"✅ Finished. Total unique members: {len(seen_member_ids)}")
        print(f"📄 Saved to: {output_file}")
            


while True:
    try:
        
        # ============================================================
        # =-=-=-=-==-=-=-=-= B1T MEMBER INFO =-=-=-=-==-=-=-=-=-=-=-=
        # ============================================================

        # Fetch._get_cookies("bobeta191.com", "b191", "thomas012", "Thomas000!@#", "/home/thomas/get_cookies/bobeta191.com.json")
        safe_call(Fetch.member_info, "bobeta191.com", "b191", "B1T", "THB", "+07:00", "B191_B1T_MI", "1lN3FXPA87TE232yOCZfrr7wkiaPMCj6EbW1dxUZ_T5rOO7nVQ4", "New Register", description="S5T member info")
        



    except KeyboardInterrupt:
            logger.info("Execution interrupted by user.")
            break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
