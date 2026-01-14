import os
import sys
import time
import json
import urllib3
import requests
from dotenv import load_dotenv
from colorama import Fore, Style
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from datetime import datetime
from dateutil.relativedelta import relativedelta
from google_auth_oauthlib.flow import InstalledAppFlow
from runtime import logger, safe_call

PROXIES = {
    "http":  "http://127.0.0.1:10809",
    "https": "http://127.0.0.1:10809",
}

def create_session():
    s = requests.Session()
    s.proxies.update(PROXIES)
    s.trust_env = False   # VERY IMPORTANT
    return s

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    }

# Google Sheet helpers
class GoogleSheetHelper:

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
    def _sort_rows_by_datetime(rows, field_name, descending=False):
        """Sort list of dict rows in-place by a datetime field."""
        def parse_dt(raw):
            if not raw:
                return datetime.min
            if isinstance(raw, datetime):
                return raw
            try:
                # handle "YYYY-MM-DD HH:MM:SS"
                return datetime.fromisoformat(str(raw))
            except ValueError:
                try:
                    return datetime.strptime(str(raw), "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return datetime.min

        def sort_key(row):
            if isinstance(row, dict):
                return parse_dt(row.get(field_name))
            return datetime.min

        rows.sort(key=sort_key, reverse=descending)

    # ====================================================================================
    # =-=-=-=-=-=-=-=-=-=-=-=-=-=-=- IBS LAST LOGIN =-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
    # ====================================================================================
    
    @classmethod
    def upload_to_google_sheet_Last_Login(cls, gs_id, gs_tab, rows):
        
        # Authenticate with OAuth2
        creds = cls.googleAPI()
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # Google Sheet ID and Sheet Tab Name (range name)
        SPREADSHEET_ID = gs_id
        RANGE_NAME = f"{gs_tab}!A2:B"

        rows = list(rows or [])
        cls._sort_rows_by_datetime(rows, "last_login_info_date")

        # Convert dict rows into GS-friendly lists
        sanitized = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            username = str(r.get("username", ""))
            last_login_raw = r.get("last_login_info_date") or ""
            try:
                dt = datetime.fromisoformat(str(last_login_raw).replace("Z", "+00:00"))
                last_login_fmt = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                last_login_fmt = str(last_login_raw)
            sanitized.append([username, last_login_fmt])

        if not sanitized:
            print("No rows found to upload to Google Sheet.")
            return

        body = {"values": sanitized, "majorDimension": "ROWS"}

        print(f"Uploading {len(sanitized)} rows to Google Sheet range {RANGE_NAME}")

        try:
            # Clear existing data before upload
            sheet.values().clear(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME
            ).execute()
            
            # Update New Data
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
        print("Uploaded data to Google Sheet.")

# Fetch Data
class Fetch(BO_Account, GoogleSheetHelper):
    
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

    # =========================== Last Login Info ===========================

    # Last Login Info
    @classmethod
    def last_Login(cls, bo_link, bo_name, team, currency, gmt_time, gs_id, gs_tab):

        session = create_session()
        
        # Print Team Name Messages
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN} {team} {Fore.YELLOW} Last Login Data...{Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        today_dt = datetime.now()                 # Dec 17 (tomorrow)
        start_dt = today_dt - relativedelta(months=1)

        start_day = start_dt.strftime("%Y-%m-%d")
        end_day = today_dt.strftime("%Y-%m-%d")

        print("Start:", start_day)
        print("End:", end_day)

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
        "paginate": 1000,
        "page": 1,
        "gmt": gmt_time,
        "currency": [
            currency
        ],
        "vip_level": [],
        "last_login_from": start_day,
        "last_login_to": end_day,
        "merchant_id": 1,
        "admin_id": 205,
        "aid": 205
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

            cls._get_cookies(
                bo_link,
                cls.accounts[bo_name]["merchant_code"],
                cls.accounts[bo_name]["acc_ID"],
                cls.accounts[bo_name]["acc_PASS"],
                f"/home/thomas/get_cookies/{bo_link}.json"
            )
            
            # Retry request...
            return cls.last_Login(bo_link, bo_name, team, currency, gmt_time, gs_id, gs_tab)

        all_rows = []

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

            # STOP if 0 rows
            if not rows:
                print(f"🛑 Page {page} returned 0 rows. Stopping pagination.")
                break

            all_rows.extend(rows)

        if all_rows:
            cls.upload_to_google_sheet_Last_Login(gs_id, gs_tab, all_rows)
        else:
            print("No rows collected to upload to Google Sheet.")

while True:
    try:

        # ==========================================================================
        # =-=-=-=-==-=-=-=-= Last Login Info =-=-=-=-==-=-=-=-=-=-= 
        # ==========================================================================

        # J1B
        safe_call(Fetch.last_Login, "batsman88.com", "jaya11", "J1B", "BDT", "+07:00", "1j4ykgy_HvReu08rmAawUs8qxwCzzn7wMs9-OmmtcINU", "J1B", description="J1B LAST LOGIN")
        # J8N
        safe_call(Fetch.last_Login, "jw8bo.com", "jw8", "J8N", "NPR", "+07:00", "1j4ykgy_HvReu08rmAawUs8qxwCzzn7wMs9-OmmtcINU", "J8N", description="J8N LAST LOGIN")
        # J1N
        safe_call(Fetch.last_Login, "batsman88.com", "jaya11", "J1N", "NPR", "+07:00", "1j4ykgy_HvReu08rmAawUs8qxwCzzn7wMs9-OmmtcINU", "J1N", description="J1N LAST LOGIN")
        # K8N
        safe_call(Fetch.last_Login, "6668889.store", "k88", "K8N", "NPR", "+07:00", "1j4ykgy_HvReu08rmAawUs8qxwCzzn7wMs9-OmmtcINU", "K8N", description="K8N LAST LOGIN")
        # I8N
        safe_call(Fetch.last_Login, "6668889.site", "i88", "I8N", "NPR", "+07:00", "1j4ykgy_HvReu08rmAawUs8qxwCzzn7wMs9-OmmtcINU", "I8N", description="I8N LAST LOGIN")
        
        # SSBO
        # safe_call(Fetch.ssbo_last_Login, "aw8", ["MYR"], "1j4ykgy_HvReu08rmAawUs8qxwCzzn7wMs9-OmmtcINU", "LAST LOGIN", description="SSBO A8M MY LAST LOGIN")

    except KeyboardInterrupt:
            logger.info("Execution interrupted by user.")
            break
    except Exception:
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
