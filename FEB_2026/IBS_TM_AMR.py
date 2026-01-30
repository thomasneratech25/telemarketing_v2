import os
import sys
import time
import pytz
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from colorama import Fore, Style
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
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
        "dis88": {
            "merchant_code": os.getenv("MERCHANT_CODE_DIS88"),
            "acc_ID": os.getenv("ACC_ID_DIS88"),
            "acc_PASS": os.getenv("ACC_PASS_DIS88")
        },
    }

# Google Sheet helpers
class gs_helpers:

    # Handles Google Sheets operations for the Telemarketing project.
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    TOKEN_PATH = "./api/google3/token.json"
    CREDS_PATH = "./api/google3/credentials.json"

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

    # Normalize rows for All Member Report
    @staticmethod
    def normalize_amr_rows(rows):
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
                skipped += 1
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

        inserted = len(cleaned_docs)
        return cleaned_docs, inserted, skipped

    # Update Data to Google Sheet (All Member Report) IBS
    @classmethod
    def upload_to_google_sheet_AMR(cls, gs_id, gs_tab, rows=None):

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


        if rows is None:
            rows = []

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
                valueInputOption="USER_ENTERED",
                body=body
            ).execute()
        except Exception as exc:
            print(f"Failed to upload to Google Sheets: {exc}")
            raise

        # print("Rows to upload:", rows)
        print("Uploaded data to Google Sheet.\n")

# Fetch Data
class Fetch(BO_Account, gs_helpers):
    
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

    # =========================== ALL MEMBER REPORT ===========================

    # BO All Member Report
    @classmethod
    def allmemberReport(cls, team, bo_link, bo_name, currency, gmt_time, g_sheet_ID, g_sheet_tab):
        
        session = create_session()
        
        # Print Color Messages
        msg = f"\n{Style.BRIGHT}{Fore.YELLOW}Getting {Fore.GREEN} {team} {Fore.YELLOW} TM AMR DATA...{Style.RESET_ALL}\n"
        for ch in msg:
            sys.stdout.write(ch)
            sys.stdout.flush()
            time.sleep(0.01)

        # Get today date
        today = datetime.now().strftime("%Y-%m-%d")

        # Get every month of first 1st
        today2 = datetime.now()
        # Replace day with 1
        first_day_of_month = today2.replace(day=1)
        # Format to YYYY-MM-DD
        first_day_of_month_str = first_day_of_month.strftime("%Y-%m-%d")

        # Cookie File
        cookie_file = f"/home/thomas/get_cookies/{bo_link}.json"

        # Auto-create file if missing
        os.makedirs(os.path.dirname(cookie_file), exist_ok=True)
        if not os.path.exists(cookie_file):
            open(cookie_file, "w").write('{"user_cookie": ""}')

        # Load cookie
        with open(cookie_file, "r", encoding="utf-8") as f:
            user_cookie = json.load(f).get("user_cookie", "")

        url = f"https://v3-bo.{bo_link}/api/be/report/get-all-member-report"

        headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'domain': f'v3-bo.{bo_link}',
            'gmt': gmt_time,
            'lang': 'en-US',
            'loggedin': 'true',
            'origin': f'https://v3-bo.{bo_link}',
            'page': '/en-us/report/personnel-report/all-member-report',
            'priority': 'u=1, i',
            'referer': f'https://v3-bo.{bo_link}/en-us/report/personnel-report/all-member-report',
            'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'type': 'POST',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
            'Cookie': f"i18n_redirected=en-us; user={user_cookie}",
        }

        page = 1
        paginate = 50000
        total_inserted = 0
        total_skipped = 0
        last_page_with_data = 0
        finish_reason = None
        by_member_id = {}
        no_id_rows = []
        no_id_keys = set()

        while True:

            payload = {
                "paginate": paginate,
                "page": page,
                "from_date": f"{first_day_of_month_str}T00:00:00+08:00",
                "to_date": f"{today}T23:59:59+08:00",
                "currency": [currency],
                "merchant_id": 1,
                "admin_id": 295,
                "aid": 295
            }

            # Post Response 
            response = session.post(url, headers=headers, json=payload, timeout=30)

            try:
                data = response.json()
            except ValueError:
                print("⚠️ Response is not valid JSON.")
                print("Status code:", response.status_code)
                print("Text preview:", response.text[:500])
                finish_reason = "invalid_json"
                break

            if data.get("statusCode") == 401:
                print("⚠️ Received 401 Unauthorized. Attempting to refresh cookies...")

                cls._get_cookies(
                    bo_link,
                    cls.accounts[f"{bo_name}"]["merchant_code"],
                    cls.accounts[f"{bo_name}"]["acc_ID"],
                    cls.accounts[f"{bo_name}"]["acc_PASS"],
                    f"/home/thomas/get_cookies/{bo_link}.json"
                )
                # Retry request...
                print("⚠️  Cookies refreshed ... Retrying request...")
                return cls.allmemberReport(team, bo_link, bo_name, currency, gmt_time, g_sheet_ID, g_sheet_tab)

            rows = data.get("data", [])
            pagination = data.get("pagination", {})

            print(f"\nPage {page} → {len(rows)} rows")

            if not isinstance(rows, list):
                rows = []

            if not rows:
                finish_reason = "no_data" if page == 1 else "completed"
                if page > 1:
                    last_page_with_data = page - 1
                break

            cleaned_rows, _inserted, skipped = cls.normalize_amr_rows(rows)
            total_skipped += skipped
            for doc in cleaned_rows:
                member_id = doc.get("member_id")
                if member_id:
                    if member_id in by_member_id:
                        total_skipped += 1
                    else:
                        total_inserted += 1
                    by_member_id[member_id] = doc
                else:
                    no_id_key = (
                        str(doc.get("username") or ""),
                        str(doc.get("fullname") or ""),
                        str(doc.get("member_id") or ""),
                    )
                    if no_id_key in no_id_keys:
                        total_skipped += 1
                    else:
                        total_inserted += 1
                        no_id_keys.add(no_id_key)
                        no_id_rows.append(doc)
            last_page_with_data = page

            pagination_last_page = None
            if isinstance(pagination, dict):
                pagination_last_page = (
                    pagination.get("last_page")
                    or pagination.get("lastPage")
                )
                if pagination_last_page is not None:
                    try:
                        pagination_last_page = int(pagination_last_page)
                    except (TypeError, ValueError):
                        pagination_last_page = None

            if (
                pagination_last_page is not None
                and page >= pagination_last_page
            ):
                finish_reason = "completed"
                break

            if pagination_last_page is None and len(rows) < paginate:
                finish_reason = "completed"
                break

            page += 1

        if finish_reason is None:
            finish_reason = "completed"

        print(f"Row Summary → Inserted: {total_inserted}, Skipped: {total_skipped}")
        if finish_reason == "no_data":
            print("⚠️ No All Member rows returned — break")
        elif finish_reason == "invalid_json":
            print("⚠️ Unable to continue due to invalid JSON response.")
        else:
            print(f"Finished. Last page = {last_page_with_data}")

        all_rows = list(by_member_id.values()) + no_id_rows
        all_rows.sort(key=lambda row: str(row.get("username") or ""))
        cls.upload_to_google_sheet_AMR(g_sheet_ID, g_sheet_tab, rows=all_rows)  

###############=================================== CODE RUN HERE =======================================############

### ==== README YO!!!! ==== ####
# member_info format = (bo link, bo name, currency, gmt time, data label, GS ID, GS Tab Name)
# member_info_2 format = (bo link, bo name, currency, gmt time, data label, GS ID, GS Tab Name)
# deposit_list format = (bo link, bo name, currency, gmt time, data label, GS ID, GS Tab Name, google sheet start column, google sheet end column)

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
        
        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS GT TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # GT (ALL MEMBER INFO) KOI
        safe_call(Fetch.allmemberReport, "IBS GT", "gcwin99bo.com", "gc99", "THB", "+08:00", "1lFrK5k8oRIW1e4aFtaLTB4dGtLnVgtwtlfMKQdtFqrc", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS N855T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # JIRAPORN
        safe_call(Fetch.allmemberReport, "IBS N855T", "f5x3n8v.com", "n855", "THB", "+08:00", "1f1k9D-IKdL8qyoQyLLBPi_ww-cTRJV4-S1B3B4lkrKM", "TM - All Member Report")
    
        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS N1T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # SUPATTA
        safe_call(Fetch.allmemberReport, "IBS N1T", "m8b4x1z6.com", "n191", "THB", "+08:00", "1tlFaPMptGcwFEp1dtzBqSTz5R8ofWhOcasYi5QBphCI", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS N789T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # N789T (ALL MEMBER INFO) SUPATTA
        safe_call(Fetch.allmemberReport, "IBS N789T", "q2n5w3z.com", "n789", "THB", "+08:00", "1H1PKac3Hd9dcIS-dTLBFMDKsCQBYUzX6PG9YCJ7hu_Y", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS S8T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # S8T (ALL MEMBER INFO) MIN
        safe_call(Fetch.allmemberReport, "IBS S8T", "siam855bo.com", "s855", "THB", "+08:00", "1MOCaMD48nxSyBMVG0O01kKL4c5KxGJz71gb5nTpVvxo", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS S6T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # S6T (ALL MEMBER INFO) PU
        safe_call(Fetch.allmemberReport, "IBS S6T", "siam66bo.com", "s66", "THB", "+08:00", "1jUdIWWG_PYZKLVDLKnDzgT-T7BVCld6REp42T9NXaBA", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS 2WT TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # 2WT (ALL MEMBER INFO) SATANG
        safe_call(Fetch.allmemberReport, "IBS 2WT", "w8c4n9be.com", "22w", "THB", "+08:00", "JgEWHohCHU-SYtY4IMVnuTTfMIYartGEuXSllLawmto", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS 2FT TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # 2FT (ALL MEMBER INFO) NING
        safe_call(Fetch.allmemberReport, "IBS 2FT", "22funbo.com", "22f", "THB", "+08:00", "1cTUwdSANPnmtLOeUd1TG6MrcF1OnJ2y2TLvAeaJ8Cww", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS S2T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # CHING
        safe_call(Fetch.allmemberReport, "IBS S2T", "m3v5r6cx.com", "s212", "THB", "+08:00", "1hXcxpefr2DdMdprn5RnTVmfRZrUBgeIh_phTYuJysvI", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS M1T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # KAEO
        safe_call(Fetch.allmemberReport, "IBS M1T", "zupra7x.com", "mf191", "THB", "+08:00", "1ccQ9N4kv9cyS9dBcmTw1jN_vY9pXBxq9RdFKmRVmp5I", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J8T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # J8T (ALL MEMBER INFO) TIP
        safe_call(Fetch.allmemberReport, "IBS J8T", "jw8bo.com", "jw8", "THB", "+08:00", "1BT35DlrXVhMxI5h2BTTa6wDoqHWkOk264BOxlLf1wBc", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J8M TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # AVA
        j8m_rows = safe_call(Fetch.allmemberReport, "IBS J8M", "jw8bo.com", "jw8", "MYR", "+08:00", "1Za_zlpRLq9fJDB7m7WWq5ob9G7fG_gPG57kSYcBI9u0", "TM - All Member Report", upload=False)
        safe_call(gs_helpers.upload_to_google_sheet_AMR, "1Za_zlpRLq9fJDB7m7WWq5ob9G7fG_gPG57kSYcBI9u0", "TM - All Member Report", rows=j8m_rows)

        # Eunice 
        safe_call(gs_helpers.upload_to_google_sheet_AMR, "1GS7Hz4r0eO3dwxHS2q_AZ5f5Oj3ABJvLYf56oPXxE9w", "TM - All Member Report", rows=j8m_rows)
        
        # XY
        safe_call(gs_helpers.upload_to_google_sheet_AMR, "16qAza8NdLf_0OhlXEC-JxMNtzE1DHPWwCnx95A0FOcI", "TM - All Member Report", rows=j8m_rows)
        
        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J8S TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        j8s_rows = safe_call(Fetch.allmemberReport, "IBS J8S", "jw8bo.com", "jw8","SGD", "+08:00", "1rB2VCsI4mw_alo5rEiofLv8sJ6TBNh-9IUcBNv6Dcno", "TM - All Member Report",upload=False)
        safe_call(gs_helpers.upload_to_google_sheet_AMR, "1rB2VCsI4mw_alo5rEiofLv8sJ6TBNh-9IUcBNv6Dcno", "TM - All Member Report", rows=j8s_rows)

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS MST TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # MST (ALL MEMBER INFO) POP
        safe_call(Fetch.allmemberReport, "IBS MST", "bo-msslot.com", "slot", "THB", "+08:00", "16IZ0K_qin81t-SWlmKAuDLXl7IZPsvP4Bhs2lRfdbIQ", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS I8T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # I8T (ALL MEMBER INFO) KUNG
        safe_call(Fetch.allmemberReport, "IBS I8T", "i828.asia", "828", "THB", "+08:00", "1yZ892mseBMkddq6JuFJtI_THMyxUD1rBejhXJUu6nw4", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS G855T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # G855T (ALL MEMBER INFO) VIEW
        safe_call(Fetch.allmemberReport, "IBS G855T", "god855.asia", "g855", "THB", "+08:00", "1kJVDdpS3rAG9f31nk96HUloT1RJspu5oOSY-Txd7Wfs", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS S345T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # BUA
        safe_call(Fetch.allmemberReport, "IBS S345T", "57249022.asia", "s345", "THB", "+08:00", "1bSWCe1ejtv_dMXJPUWxqlvxYXWId_9J1Ddb5cwBOlKE", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS A8N TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # A8N (ALL MEMBER INFO) ANNA
        safe_call(Fetch.allmemberReport, "IBS A8N", "aw8bo.com", "aw8", "NPR", "+08:00", "1kw12XtEgWJpUzvNpl9IwRrc66OiWWwCXkGfPbJ_nmL0", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS A8M TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # ANNA
        a8m_rows = safe_call(Fetch.allmemberReport, "IBS A8M", "aw8bo.com", "aw8", "MYR", "+08:00", "1PZ0OMMEV_p-wyPv_Sk2D1nNABlDGlUGMI-U-kfjt0as", "TM - All Member Report (IBS)", upload=False)
        safe_call(gs_helpers.upload_to_google_sheet_AMR, "1PZ0OMMEV_p-wyPv_Sk2D1nNABlDGlUGMI-U-kfjt0as", "TM - All Member Report (IBS)", rows=a8m_rows)

        # Angie
        safe_call(gs_helpers.upload_to_google_sheet_AMR, "1xodhapPpnOHXgFWYGuG6fWCGoVKUZ6Y_hC5HmpvMdzg", "TM - All Member Report (IBS)", rows=a8m_rows)
        
        # Angie2
        safe_call(gs_helpers.upload_to_google_sheet_AMR, "1e-Fhwyc0yON1IBnzyHmBITujmSzZdci1rzOzL4urXa4", "TM - All Member Report (IBS)", rows=a8m_rows)

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS A8S TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # AVA
        a8s_rows = safe_call(Fetch.allmemberReport, "IBS A8S", "aw8bo.com", "aw8", "SGD", "+08:00", "1b2yE7mdtxXxa-lLhgGX_ciVHIyWTzy4MUgMfHnHDqRk", "TM - All Member Report (IBS)", upload=False)
        safe_call(gs_helpers.upload_to_google_sheet_AMR, "1b2yE7mdtxXxa-lLhgGX_ciVHIyWTzy4MUgMfHnHDqRk", "TM - All Member Report (IBS)", rows=a8s_rows)
        
        # Cindy
        safe_call(gs_helpers.upload_to_google_sheet_AMR, "1UMz3hMfPhvfrZaudJvkYI8gjh1_GOXI6a2ZlibeFAL0", "TM - All Member Report (IBS)", rows=a8s_rows)

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J1B TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # J1B (ALL MEMBER INFO) RABBY
        safe_call(Fetch.allmemberReport, "IBS J1B", "batsman88.com", "jaya11", "BDT", "+08:00", "1NaxtKUkQOsdwFmqDrv_yEnMNW9GST9GLn2_p5x0PsIw", "TM - All Member Report")
        
        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS I8N TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # I8N (ALL MEMBER INFO) LOKENDRA
        safe_call(Fetch.allmemberReport, "IBS I8N", "6668889.site", "i88", "NPR", "+08:00", "1-7UObibmsw2vNogMyXhnaP2SDP56iwPgYIeG-5zJ3jc", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J1N TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # J1N (ALL MEMBER INFO) LAXMI
        safe_call(Fetch.allmemberReport, "IBS J1N", "batsman88.com", "jaya11", "NPR", "+08:00", "1to0EnUiUirgAzmF-cUwHFRLYPGO3OeL8tqWxqaafhC8", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J8N TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # J8N (ALL MEMBER INFO) SIJAPATI
        safe_call(Fetch.allmemberReport, "IBS J8N", "jw8bo.com", "jw8", "NPR", "+08:00", "11IIc3Y1aS6DcQTyITraQCT9jC5dekfwfScwocWWL7FA", "TM - All Member Report")
        
        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS J8B TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # J8B (ALL MEMBER INFO) ALI
        safe_call(Fetch.allmemberReport, "IBS J8B", "jw8bo.com", "jw8", "BDT", "+08:00", "15mL4VqEz1bdRfWQQBAXz8AMUofBIe3HVetl_N1hbncw", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS K8N TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # K8N (ALL MEMBER INFO) HIMANI
        safe_call(Fetch.allmemberReport, "IBS K8N", "6668889.store", "k88", "NPR", "+08:00", "1gmzY97EWfOdo-3c1Lmvt15aInG6PdUSavnzattkOamc", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS D8M TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # D8M (ALL MEMBER INFO) RY
        safe_call(Fetch.allmemberReport, "IBS D8M", "dis88bo.com", "dis88", "MYR", "+08:00", "1eGdpMBnOLCT3pE1pyWm0RS4_jyhbBZqY1ChHb_Y9rYA", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS R99T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # R99T (POP)
        safe_call(Fetch.allmemberReport, "IBS R99T", "bex7tor.com", "r99", "THB", "+08:00", "1RtAI_Z3qPe3O_cw7AnP9x0geUuzlEd7Kr0primEolRU", "TM - All Member Report")

        # =============================================================================================================================
        # -_-_-_-_-_-_-_-_-_-_-_-_-_-_  IBS S1T TM ALL MEMBER REPORT -_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_
        # =============================================================================================================================

        # S1T (PHOUNG)
        safe_call(Fetch.allmemberReport, "IBS S1T", "sol9ven.com", "s191", "THB", "+08:00", "1LCOw9dCu7CW-acsIZkCEzP_DvemZEC7EeSRuZn0n2vY", "TM - All Member Report")

    except KeyboardInterrupt:
        logger.info("Execution interrupted by user.")
        break
    except Exception:   
        logger.exception("Unexpected error; retrying in 60 seconds.")
        time.sleep(60)
