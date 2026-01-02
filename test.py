import os
import requests
import json
import certifi

url = "https://v3-bo.m8b4x1z6.com/api/be/finance/get-deposit"

payload = json.dumps({
  "paginate": 10,
  "page": 1,
  "currency": [
    "THB"
  ],
  "status": "approved",
  "start_date": "2026-01-01",
  "end_date": "2026-01-02",
  "gmt": "+08:00",
  "merchant_id": 1,
  "admin_id": 243,
  "aid": 243
})
headers = {
  'accept': 'application/json',
  'accept-language': 'en-US,en;q=0.9',
  'cache-control': 'no-cache',
  'content-type': 'application/json',
  'domain': 'v3-bo.m8b4x1z6.com',
  'gmt': '+08:00',
  'lang': 'en-US',
  'loggedin': 'true',
  'origin': 'https://v3-bo.m8b4x1z6.com',
  'page': '/en-us/finance-management/deposit',
  'pragma': 'no-cache',
  'priority': 'u=1, i',
  'referer': 'https://v3-bo.m8b4x1z6.com/en-us/finance-management/deposit',
  'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"macOS"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'same-origin',
  'type': 'POST',
  'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
  'Cookie': 'i18n_redirected=en-us; __cf_bm=Vok0sIBcHhnVzUt1sAc94JUEPA3MANq4v8DnEixfoqY-1767319737-1.0.1.1-gwDmxYlCrzW2IWdtCbnXwdu2hEipxOASNQz7ywUgcW26xgblRbEWzt9ODdW7vLGYdzlt0v0PL4hpZzTiN2jS6rwIbwcLo_12QkhFdN_dsjk; user=eyJ0b2tlbiI6ImZmMWZkOWRlZWYzN2NhMjQ3ODY2YTg2MjFmMDA4ZjY1IiwiZGF0YSI6eyJpZCI6MjQzLCJtZXJjaGFudF9pZCI6MSwibmFtZSI6InRob21hczAxMiIsInVzZXJuYW1lIjoidGhvbWFzMDEyIiwibWVyY2hhbnRfY3JlZGl0X2FjY2VzcyI6IlRIQiIsImxvZ2luX3RpbWUiOiIyMDI2LTAxLTAyVDAyOjA5OjEyLjA3NzUxMloiLCJzdGF0dXMiOjEsImFkbWluX3R5cGUiOiJhZG1pbiIsInJlbWFyayI6bnVsbCwibWVyY2hhbnRfbmFtZSI6Im4xOTEiLCJtZW51RmxhZyI6MSwiaXNfZm9yY2UiOjAsImRlcGFydG1lbnRfaWQiOjg4LCJnb29nbGVfbG9naW5fYXV0aGVudGljYXRpb24iOjAsImVuY3J5cHRlZF90b2tlbiI6IjVaNEdCTk1wbEVOVm5kQ05qTkVXZmpoaVYxVjBWbmRVVUVaT2RrZzBjMWhPUWxaTlFVOWFValJGV0dKdlNXbHVRVU5MWXpkNWFHUktNRU5FZGtOVEsyOVpSRXA0VFZOYU0za3lhREJHVldnPSIsImxhaXZlY2hhdF90b2tlbiI6IlhQWmJ3MmRkbnB1dE80YnRWc0hINkxleE9yS2x2Q2l1a2ZBMzNKZ1JSQTNvZnkvc1RKeFNxVk5QRUhNcGE2UE9jc2tPeWxyVS9RS1hXRE9lV1NEZ1pnPT0ifX0%3D; __cf_bm=HB6.Uk1KAlvYJqT14n4RAS7patud.FzW5qq93E5b98E-1767319835-1.0.1.1-21uwf.R_qw2FQ1lEGQKtOTj8vOavkSG8X_7JLRr3LqMlCoB0bZkyS0EvYENfvjLETplb.FshtvxtGt2gjVIbm83IVP7ABoUeuppgh9gocmI'
}

ca_bundle = os.environ.get("REQUESTS_CA_BUNDLE") or os.environ.get("SSL_CERT_FILE")
local_ca = os.path.join(os.path.dirname(__file__), "ca_bundle.pem")
if ca_bundle:
    verify_path = ca_bundle
elif os.path.isfile(local_ca):
    verify_path = local_ca
else:
    verify_path = certifi.where()

response = requests.request("POST", url, headers=headers, data=payload, verify=verify_path)

print(response.text)
