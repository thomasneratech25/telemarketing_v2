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

        # Call MongoDB database and collection
        client = MongoClient(MONGODB_URI)
        db = client["CONVERSION"]
        collection = db[collection]

        # Set and Ensure when upload data this 3 Field is Unique Data
        collection.create_index(
            [("member_id", 1)],
            unique=True
        )

        # Count insert and skip
        inserted = 0
        skipped = 0
        cleaned_docs = []

        batch = []

        # for each rows in a list of JSON objects return
        for row in rows:

            # Skip null or invalid rows
            if not isinstance(row, dict):
                continue

            # Extract only the fields you want (Extract Data from json file)
            username= row.get("username")
            first_name = row.get("first_name")
            register_info_date = row.get("register_info_date")
            mobileno = row.get("mobileno")
            member_id = row.get("member_id")
            email = row.get("email")

            # Convert Date and Time to (YYYY-MM-DD HH:MM:SS)
            dt = datetime.fromisoformat(register_info_date)
            register_info_date_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")

            # Build the new cleaned document (Use for upload data to MongoDB)
            doc = {
                "username": username,
                "first_name": first_name,
                "register_info_date": register_info_date_fmt,
                "mobileno": mobileno,
                "member_id": member_id,
                "email": email,
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

            # STOP only after double-checking the next page
            if not rows:
                print(f"⚠️ Page {page} returned 0 rows — double checking Page {page + 1}...")

                # Prepare next-page payload
                next_payload = payload.copy()
                next_payload["page"] = page + 1

                # Request next page
                next_response = session.post(url, headers=headers, json=next_payload, timeout=30)
                
                try:
                    next_data = next_response.json()
                    next_rows = next_data.get("data", [])
                except Exception:
                    print("⚠️ Invalid JSON while double checking next page.")
                    next_rows = []

                print(f"Page {page + 1} → {len(next_rows)} rows (double check)")

                # Only break if next page *also* returns 0
                if not next_rows:
                    print(f"No transaction ID found on Page {page} and Page {page + 1}. Breaking loop.")
                    break
                else:
                    print(f"Page {page} was empty but Page {page + 1} has data → continuing...")
                    continue

            # Insert into MongoDB
            if "data" in data and len(data["data"]) > 0:
                cls.mongodbAPI_DL_PID(data["data"], collection)
            else:
                print("No data returned from API.")

        # # Upload Data to Google Sheet by reading from MongoDB
        mongodb_2_gs.upload_to_google_sheet_DL_PID(collection, gs_id, gs_tab, start_column, end_column)

        # Python Requests Method
    
    # =========================== Member Info ===========================

    # Member Info
    @classmethod
    def member_info(cls, bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab):
        
        session = create_session()

        # Print Team Name Messages
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN} {team} {Fore.YELLOW} MEMBER INFO Data...{Style.RESET_ALL}\n"
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
        # 00:00 - 01:00 → use yesterday
        # 01:01 onward → use today
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
    

        url = f"https://v3-bo.{bo_link}/api/be/member/get-list"

        payload = {
        "paginate": 10000,
        "page": 1,
        "gmt": gmt_time,
        "currency": [
            currency
        ],
        "register_from": start_date,
        "register_to": end_date,
        "merchant_id": 1,
        "admin_id": 581,
        "aid": 581
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
        'page': '/en-us/member/list',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://v3-bo.{bo_link}/en-us/member/list',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-arch': '"arm"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"142.0.7444.162"',
        'sec-ch-ua-full-version-list': '"Chromium";v="142.0.7444.162", "Google Chrome";v="142.0.7444.162", "Not_A Brand";v="99.0.0.0"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"macOS"',
        'sec-ch-ua-platform-version': '"15.5.0"',
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
            return cls.member_info(bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab)

        # For loop page and fetch data
        for page in range(1, 10000): 

            payload["page"] = page

            # Send POST request (CORRECT WAY)
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

            # STOP when no data
            if not rows:
                print(f"Finished. Last page = {page-1}")
                break

            # Insert into MongoDB
            if "data" in data and len(data["data"]) > 0:
                cls.mongodbAPI_MI(data["data"], collection)
            else:
                print("No data returned from API.")

        # Upload Data to Google Sheet by reading from MongoDB
        mongodb_2_gs.upload_to_google_sheet_MI(collection, gs_id, gs_tab)

    # Member Info (Split data)
    @classmethod
    def member_info_SPLIT_DATA(cls, bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab):

        session = create_session()

        # Print Team Name Messages
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN} {team} {Fore.YELLOW} MEMBER INFO Data...{Style.RESET_ALL}\n"
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
        # 00:00 - 01:00 → use yesterday
        # 01:01 onward → use today
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

        url = f"https://v3-bo.{bo_link}/api/be/member/get-list"

        payload = {
        "paginate": 10000,
        "page": 1,
        "gmt": gmt_time,
        "currency": [
            currency
        ],
        "register_from": start_date,
        "register_to": end_date,
        "merchant_id": 1,
        "admin_id": 581,
        "aid": 581
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
        'page': '/en-us/member/list',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': f'https://v3-bo.{bo_link}/en-us/member/list',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-arch': '"arm"',
        'sec-ch-ua-bitness': '"64"',
        'sec-ch-ua-full-version': '"142.0.7444.162"',
        'sec-ch-ua-full-version-list': '"Chromium";v="142.0.7444.162", "Google Chrome";v="142.0.7444.162", "Not_A Brand";v="99.0.0.0"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-model': '""',
        'sec-ch-ua-platform': '"macOS"',
        'sec-ch-ua-platform-version': '"15.5.0"',
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
            return cls.member_info_SPLIT_DATA(bo_link, bo_name, team, currency, gmt_time, collection, gs_id, gs_tab)
            
        # For loop page and fetch data
        for page in range(1, 10000): 

            payload["page"] = page

            # Send POST request (CORRECT WAY)
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

            # STOP when no data
            if not rows:
                print(f"Finished. Last page = {page-1}")
                break

            # Insert into MongoDB
            if "data" in data and len(data["data"]) > 0:
                cls.mongodbAPI_MI(data["data"], collection)
            else:
                print("No data returned from API.")

        # Upload Data to Google Sheet by reading from MongoDB
        mongodb_2_gs.upload_to_google_sheet_MI_SPLIT_DATA(collection, gs_id, gs_tab)

# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= CODE RUN HERE =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-= 
# -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

### ==== 🥹🥹🥹 README 🥹🥹🥹 ==== ####
### ==== 🥹🥹🥹 README 🥹🥹🥹 ==== ####
### ==== 🥹🥹🥹 README 🥹🥹🥹 ==== ####

#### FORMAT ---NEW REGISTER & DEPOSIT LIST--- FORMAT ####
# safe_call(Fetch.member_info, "BO Link", "merchant code", "team name", "currency", "gmt time", "database name", "google sheet link", "google sheet tab name", description="can write any")
# safe_call(Fetch.deposit_list_PID, "BO Link", "merchant code", "team name", "currency", "gmt time", "database name", "google sheet link", "google sheet tab name", "A", "C", description="can write any")

while True:
    try:

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= JOLIBEE MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=
        # ==========================================================================

        # Krisitian & Amber (MEMBER INFO)
        gs_ids = ["1GD4AJU4hmP6JYRnhe3dTrIuN5Nma8Ab2XGPO-lCo_SU", "1QqtIX4N1Lqij3MKSS5YBI5fIuPARjbWCipN5KjHW6II"]
        safe_call(Fetch.member_info_SPLIT_DATA, "jolibetbo.com", "joli", "Kristian & Amber", "PHP", "+08:00", "JOLI_MI", gs_ids, "New Register", description="Jolibet Kristian & Amber member info")

        # # Krisitian & Amber (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "jolibetbo.com", "joli", "Joli Kristian", "PHP", "+08:00", "JOLI_DL", "1GD4AJU4hmP6JYRnhe3dTrIuN5Nma8Ab2XGPO-lCo_SU", "DEPOSIT LIST", "A", "C", description="Jolibet Kristian deposit list")
        # safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "JOLI_DL", "1QqtIX4N1Lqij3MKSS5YBI5fIuPARjbWCipN5KjHW6II", "DEPOSIT LIST", "A", "C")
    
        # # ==========================================================================
        # # =-=-=-=-==-=-=-=-= S5T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # # ==========================================================================

        # # S5T (MEMBER INFO)
        safe_call(Fetch.member_info, "s55bo.com", "s55", "S5T", "THB", "+07:00", "S55_S5T_MI", "1lN3FXPA87TyOCZ7wkiaPMCj6EbW1dxUZ_T5rOO7nVQ4", "New Register", description="S5T member info")
        
        # S5T (DEPOSIT LIST)
        safe_call(Fetch.deposit_list_PID, "s55bo.com", "s55", "S5T", "THB", "+07:00", "S55_S5T_DL", "1lN3FXPA87TyOCZ7wkiaPMCj6EbW1dxUZ_T5rOO7nVQ4", "DEPOSIT LIST", "A", "C", description="S5T deposit list")

        # # ==========================================================================
        # # =-=-=-=-==-=-=-=-= N8Y MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # # ==========================================================================

        # N8Y (MEMBER INFO)
        safe_call(Fetch.member_info, "nex8bo.com", "nex8", "N8Y", "MMK", "+07:00", "NEX8_N8Y_MI", "1SAmpSWfRwVyM9G2L_6ZlBgQuEBL1Mxn-6iBNVJlCzuc", "New Register", description="N8Y member info")
        
        # N8Y (DEPOSIT LIST)
        safe_call(Fetch.deposit_list_PID, "nex8bo.com", "nex8", "N8Y", "MMK", "+07:00", "NEX8_N8Y_DL", "1SAmpSWfRwVyM9G2L_6ZlBgQuEBL1Mxn-6iBNVJlCzuc", "DEPOSIT LIST", "A", "C", description="N8Y deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S345T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # S345T (MEMBER INFO)
        safe_call(Fetch.member_info, "57249022.asia", "s345", "S345T", "THB", "+07:00", "S345_S345T_MI", "14aF0_HknwdDKrFOKR8ZyXUU-6qNcpIgPVgJ2bKPKhks", "New Register", description="S345T member info")
        
        # S345T (DEPOSIT LIST)
        safe_call(Fetch.deposit_list_PID, "57249022.asia", "s345", "S345T", "THB", "+07:00", "S345_S345T_DL", "14aF0_HknwdDKrFOKR8ZyXUU-6qNcpIgPVgJ2bKPKhks", "DEPOSIT LIST", "A", "C", description="S345T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S2T MEMBER INFO & DEPOSIT LIST (CHING)=-=-=-=-==-=-=-=-
        # ==========================================================================

        # S2T (MEMBER INFO) YOD
        safe_call(Fetch.member_info, "m3v5r6cx.com", "s212", "S2T", "THB", "+07:00", "S212_S2T_MI", "1Eel69vi1bBw96PeF_PQ3M_1IuziiOZhPwmmEfQeWlHY", "New Register", description="S2T member info")
        
        # S2T (DEPOSIT LIST) YOD
        safe_call(Fetch.deposit_list_PID, "m3v5r6cx.com", "s212", "S2T", "THB", "+07:00", "S212_S2T_DL", "1Eel69vi1bBw96PeF_PQ3M_1IuziiOZhPwmmEfQeWlHY", "DEPOSIT LIST", "A", "C", description="S2T deposit list")

        # S2T (DEPOSIT LIST) CHING
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "S212_S2T_DL", "1x4Uuh28P8wh0yzOfv0eQUWcIv8l9KdmSRUibSQsU1CM", "DEPOSIT LIST", "A", "C", description="S2T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S369T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # S369T (MEMBER INFO)
        safe_call(Fetch.member_info, "uhy3umx.com", "s369", "S369T", "THB", "+07:00", "S369_S369T_MI", "1lwnVV6rozzk0rgPraUTqZlnp-O7ToNjSDsi8z6oH9NI", "New Register", description="S369T member info")
        
        # S369T (DEPOSIT LIST) NOON
        safe_call(Fetch.deposit_list_PID, "uhy3umx.com", "s369", "S369T", "THB", "+07:00", "S369_S369T_DL", "1lwnVV6rozzk0rgPraUTqZlnp-O7ToNjSDsi8z6oH9NI", "DEPOSIT LIST", "A", "C", description="S369T deposit list")

        # S369T (DEPOSIT LIST) MINT
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "JW8_J8T_DL", "1AJxM5m0yOoNk7G8e8S7OdJi2lTK0zVLoiZYGp_on1D8", "DEPOSIT LIST", "A", "C", description="S369T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S6T MEMBER INFO & DEPOSIT LIST (MEI) =-=-=-=-==-=-=-=-=
        # ==========================================================================

        # S6T (MEMBER INFO)
        safe_call(Fetch.member_info, "siam66bo.com", "s66", "S6T", "THB", "+07:00", "S66_S6T_MI", "1vHhNv5IcpEvFBYJnXkpeGfjsdzemws-QaD0jjkPaqbg", "New Register", description="S6T member info")
        
        # S6T (DEPOSIT LIST) HOM
        safe_call(Fetch.deposit_list_PID, "siam66bo.com", "s66", "S6T", "THB", "+07:00", "S66_S6T_DL", "1vHhNv5IcpEvFBYJnXkpeGfjsdzemws-QaD0jjkPaqbg", "DEPOSIT LIST", "A", "C", description="S6T deposit list")

        # S6T (DEPOSIT LIST) MEI
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "S66_S6T_DL", "1C9g-iHQDQ0fZJM8tFcr2ZTTD8J5JJdZ7H5iFfUphCYA", "DEPOSIT LIST", "A", "C", description="S6T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= J8T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # J8T (MEMBER INFO)
        safe_call(Fetch.member_info, "jw8bo.com", "jw8", "J8T", "THB", "+07:00", "JW8_J8T_MI", "1cBzPLsPpUnv80Yolx4UNkurtF1kNwOhTlajE5bR5gQ4", "New Register", description="J8T member info")
        
        # J8T (DEPOSIT LIST) YING
        safe_call(Fetch.deposit_list_PID, "jw8bo.com", "jw8", "J8T", "THB", "+07:00", "JW8_J8T_DL", "1cBzPLsPpUnv80Yolx4UNkurtF1kNwOhTlajE5bR5gQ4", "DEPOSIT LIST", "A", "C", description="J8T deposit list")

        # J8T (DEPOSIT LIST) TIP
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "JW8_J8T_DL", "18EoO-iOaMwIJvnLoG59tzSNgBQP5FSAdoonRJ3KKwFE", "DEPOSIT LIST", "A", "C", description="J8T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= MST MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # MST (MEMBER INFO) FON
        safe_call(Fetch.member_info, "bo-msslot.com", "slot", "MST", "THB", "+07:00", "SLOT_MST_MI", "11gXhX4W221XMVK9qwuw1PHeCvAq0WDhjn-m0csMVb5Q", "New Register", description="MST member info")
        
        # MST (DEPOSIT LIST) FON
        safe_call(Fetch.deposit_list_PID, "bo-msslot.com", "slot", "MST", "THB", "+07:00", "SLOT_MST_DL", "11gXhX4W221XMVK9qwuw1PHeCvAq0WDhjn-m0csMVb5Q", "DEPOSIT LIST", "A", "C", description="MST deposit list")

        # MST (DEPOSIT LIST) POP
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "JW8_J8T_DL", "1D_aZd_xkUmw60RxKKMBzgKLJplxSom02nKMbUoIS-cQ", "DEPOSIT LIST", "A", "C", description="G855T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= I8T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # I8T (MEMBER INFO)
        safe_call(Fetch.member_info, "i828.asia", "828", "I8T", "THB", "+07:00", "I828_I8T_MI", "1CLx8eDoAOMTf739rPDpxUxEMuTQucTWA2ASfX7s4HUU", "New Register", description="I8T member info")
        
        # I8T (DEPOSIT LIST) TATA
        safe_call(Fetch.deposit_list_PID, "i828.asia", "828", "I8T", "THB", "+07:00", "I828_I8T_DL", "1CLx8eDoAOMTf739rPDpxUxEMuTQucTWA2ASfX7s4HUU", "DEPOSIT LIST", "A", "C", description="I8T deposit list")

        # I8T (DEPOSIT LIST) KUNG
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "I828_I8T_DL", "1vvGZ3BWzDAwWAJDsnVtzlupqTOMGEEDGO644_QnU3aU", "DEPOSIT LIST", "A", "C", description="I8T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= G855T MEMBER INFO & DEPOSIT LIST (MEME) =-=-=-=-==-=-=-
        # ==========================================================================
        
        # G855T (MEMBER INFO) VIEW
        safe_call(Fetch.member_info, "god855.asia", "g855", "G855T", "THB", "+07:00", "G855_G855T_MI", "1ijHSzjqJ-DeMzUs8msUReehOwUZiS2GESyG-mfTUqDE", "New Register", description="G855T member info")
        
        # G855T (DEPOSIT LIST) VIEW
        safe_call(Fetch.deposit_list_PID, "god855.asia", "g855", "G855T", "THB", "+07:00", "G855_G855T_DL", "1ijHSzjqJ-DeMzUs8msUReehOwUZiS2GESyG-mfTUqDE", "DEPOSIT LIST", "A", "C", description="G855T deposit list")

        # G855T (DEPOSIT LIST) VIEW
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "G855_G855T_DL", "1TIzhKJYeGaPkP_UmPOooTxrJBJ99J9Fi_765c_QsiBw", "DEPOSIT LIST", "A", "C", description="G855T deposit list")

        # G855T (MEMBER INFO) MEME
        safe_call(mongodb_2_gs.upload_to_google_sheet_MI, "G855_G855T_MI", "1B37JEPVLNEcYkdLcHRghucxykBLJSUwTt9TAx5vbXc4", "New Register", description="G855T member info")

        # G855T (DEPOSIT LIST) MEME
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "G855_G855T_DL", "1B37JEPVLNEcYkdLcHRghucxykBLJSUwTt9TAx5vbXc4", "DEPOSIT LIST", "A", "C", description="G855T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= 2FT MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # 2FT (MEMBER INFO)
        safe_call(Fetch.member_info, "22funbo.com", "22f", "2FT", "THB", "+07:00", "22F_2FT_MI", "1KzVuGBWu56sBuD1VFTXFxQi2LQ2h0M5vxb8s8bzBhtE", "New Register", description="2FT member info")
        
        # 2FT (DEPOSIT LIST) DAOW
        safe_call(Fetch.deposit_list_PID, "22funbo.com", "22f", "2FT", "THB", "+07:00", "22F_2FT_DL", "1KzVuGBWu56sBuD1VFTXFxQi2LQ2h0M5vxb8s8bzBhtE", "DEPOSIT LIST", "A", "C", description="2FT deposit list")

        # 2FT (DEPOSIT LIST) MUAY
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "22F_2FT_DL", "1GqmDUERmWKZWUPFbyvUMLUYQMJfGkdsRFON5HHRESKM", "DEPOSIT LIST", "A", "C", description="2FT deposit list")

        # 2FT (DEPOSIT LIST) AYE
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "22F_2FT_DL", "1ozCbjUYlqLIfBRw-fDqvqc1dYUXdRJBiwneFBWS6HCk", "DEPOSIT LIST", "A", "C", description="2FT deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= M1T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # M1T (MEMBER INFO) ALICE
        safe_call(Fetch.member_info, "zupra7x.com", "mf191", "M1T", "THB", "+07:00", "MF191_M1T_MI", "1Vc6ZogRmZChCD7PJcIv854nO3YEvh0gYuGEm4CGJsEk", "New Register", description="M1T member info")
        
        # M1T (DEPOSIT LIST) ALICE
        safe_call(Fetch.deposit_list_PID, "zupra7x.com", "mf191", "M1T", "THB", "+07:00", "MF191_M1T_DL", "1Vc6ZogRmZChCD7PJcIv854nO3YEvh0gYuGEm4CGJsEk", "DEPOSIT LIST", "A", "C", description="M1T deposit list")
        
        # M1T (DEPOSIT LIST) KAEO
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "MF191_M1T_DL", "1hqtyqLpTN7wXprHVBwdeUlofmFEHcl5c8ovnfO2MV-Q", "DEPOSIT LIST", "A", "C", description="M1T deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= 2WT MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # 2WT (MEMBER INFO) NONG SI
        safe_call(Fetch.member_info, "w8c4n9be.com", "22w", "2WT", "THB", "+07:00", "22W_2WT_MI", "18HSa79DT3rfXKx8KyObQ2klmSuYcChfmIwtA9Ay_z5o", "New Register", description="2WT member info")
        
        # 2WT (DEPOSIT LIST) NONG SI
        safe_call(Fetch.deposit_list_PID, "w8c4n9be.com", "22w", "2WT", "THB", "+07:00", "22W_2WT_DL", "18HSa79DT3rfXKx8KyObQ2klmSuYcChfmIwtA9Ay_z5o", "DEPOSIT LIST", "A", "C", description="2WT deposit list")
        
        # 2WT (DEPOSIT LIST) SAI
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "22W_2WT_DL", "1_17gv5_EM0BaAAHWWobuT88s55duw62uXQ9NHxakKvI", "DEPOSIT LIST", "A", "C", description="2WT deposit list")

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= S8T MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # S8T (MEMBER INFO) NINEW
        safe_call(Fetch.member_info, "siam855bo.com", "s855", "S8T", "THB", "+07:00", "S855_S8T_MI", "1qpYkDUfTK40VZk5upIKcJj5gIesCnCKD3Tx8Mi-xZFw", "New Register", description="S8T member info")
        
        # S8T (DEPOSIT LIST) NINEW
        safe_call(Fetch.deposit_list_PID, "siam855bo.com", "s855", "S8T", "THB", "+07:00", "S855_S8T_DL", "1qpYkDUfTK40VZk5upIKcJj5gIesCnCKD3Tx8Mi-xZFw", "DEPOSIT LIST", "A", "C", description="S8T deposit list")

        # S8T (DEPOSIT LIST) MEW
        safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "S855_S8T_DL", "1NwdpMcpwh4pMz5bSPxyRcsEZ4_U9CdGU-2YvGYMpJks", "DEPOSIT LIST", "A", "C", description="S8T deposit list")

        # # ==========================================================================
        # # =-=-=-=-==-=-=-=-= A8N MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # # ==========================================================================

        # # A8N  (MEMBER INFO)
        # safe_call(Fetch.member_info, "aw8bo.com", "aw8", "A8N", "NPR", "+07:00", "AW8_A8N_MI", "1XH9TYpgF0LYqs9QEnOxxvdv8QW18JdivSV_hudSifUs", "New Register", description="A8N member info")
        
        # # A8N  (DEPOSIT LIST)
        # safe_call(Fetch.deposit_list_PID, "aw8bo.com", "aw8", "A8N", "NPR", "+07:00", "AW8_A8N_DL", "1XH9TYpgF0LYqs9QEnOxxvdv8QW18JdivSV_hudSifUs", "DEPOSIT LIST", "A", "C", description="A8N deposit list")

        # # ==========================================================================
        # # =-=-=-=-==-=-=-=-= J1B MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # # ==========================================================================

        # # J1B (MEMBER INFO) (HAFIZUR)
        # safe_call(Fetch.member_info, "batsman88.com", "jaya11", "J1B", "BDT", "+07:00", "J1B_MI", "1ZOv7AniBdas5rwvnmqvrm6y0a2-czdVSd2xMzLgAhjY", "New Register", description="IBS J1B MEMBER INFO")
            
        # # J1B (DEPOSIT LIST) (HAFIZUR)
        # safe_call(Fetch.deposit_list_PID, "batsman88.com", "jaya11", "J1B", "BDT", "+07:00", "J1B_DL", "1ZOv7AniBdas5rwvnmqvrm6y0a2-czdVSd2xMzLgAhjY", "DEPOSIT LIST", "A", "C", description="J1B deposit list")

        # # J1B (DEPOSIT LIST) (ALAMGIR)
        # safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "J1B_DL", "1y8_EFXrMohv4ApBZXuvq9pMEz2288pcRZtj1MSE3E7o", "DEPOSIT LIST", "A", "C", description="J1B deposit list")
        
        # # ==========================================================================
        # # =-=-=-=-==-=-=-=-= J1N MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # # ==========================================================================
        
        # # HAVEN SETUP!!!!!!
        # # # J1N (MEMBER INFO) (BADAL) 
        # # safe_call(Fetch.member_info, "dis88bo.com", "dis88", "D8N", "MYR", "+08:00", "D8M_MI", "1iw0jWV7UHUhQ6bTrcBiFYsvCGN02e8kDtBnhKD6agu4", "New Register", description="IBS D8M MEMBER INFO")
            
        # # J1N (DEPOSIT LIST) (BADAL)
        # safe_call(Fetch.deposit_list_PID, "batsman88.com", "jaya11", "J1N", "NPR", "+07:00", "J1N_DL", "1jek1Aztz1jgvbOmHUX5SLCVl24NY1ZLDAJhS00InJUw", "DEPOSIT LIST", "A", "C", description="J1N deposit list")

        # # ==========================================================================
        # # =-=-=-=-==-=-=-=-= J8N MEMBER INFO & DEPOSIT LIST =-=-=-=-==-=-=-=-=-=-= 
        # # ==========================================================================

        # # J8N (MEMBER INFO) (LOKENDRA)
        # safe_call(Fetch.member_info, "jw8bo.com", "jw8", "J8N", "NPR", "+07:00", "J8N_MI", "1HD8-yQ1whVvEUkJFVRAPJgWj4wNlEp88LpsHielS7VA", "New Register", description="IBS J8N MEMBER INFO")
            
        # # J8N (DEPOSIT LIST) (LOKENDRA)
        # safe_call(Fetch.deposit_list_PID, "jw8bo.com", "jw8", "J8N", "NPR", "+07:00", "J8N_DL", "1HD8-yQ1whVvEUkJFVRAPJgWj4wNlEp88LpsHielS7VA", "DEPOSIT LIST", "A", "C", description="J8N deposit list")

        # # J8N (DEPOSIT LIST) (LAXMI)
        # safe_call(mongodb_2_gs.upload_to_google_sheet_DL_PID, "J8N_DL", "1eIoVnWjmhjmZTZ9SLHE8ifEuLdOtQicGVdI_s3WQoEw", "DEPOSIT LIST", "A", "C", description="J8N deposit list")


        # Delay 3 minutes
        time.sleep(180)

    except KeyboardInterrupt:
            logger.info("Execution interrupted by user.")
            break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
