import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
import random

# --- CONFIG ---
API_KEY = "c93c511dbd714335a47ba18cee241a95"
LIST_IDS = {
    "UK verified": "1131223",
    "US verified": "1131249",
    "UK unverified": "1131240",
    "US unverified": "1131144"
}
SHEET_NAME = "Email List"
SEND_BATCH_LIMIT = 10  # Adjust as needed for live/prod
TEST_MODE = True  # Set to False to actually send emails

# --- AUTHENTICATE GOOGLE SHEETS ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("client_secret.json", scope)
gc = gspread.authorize(creds)

# --- FUNCTIONS ---
def send_to_acumbamail(contact, list_id):
    email = contact.get("email")
    if not email:
        print("⚠️ Skipping row with no email.")
        return False

    files = {
        "auth_token": (None, API_KEY),
        "list_id": (None, list_id),
        "merge_fields[EMAIL]": (None, email),
        "merge_fields[FIRSTNAME]": (None, contact.get("First Name", "")),
        "merge_fields[LASTNAME]": (None, contact.get("Last Name", "")),
        "merge_fields[COMPANYNAME]": (None, contact.get("Company Name", "")),
        "merge_fields[ROLE]": (None, contact.get("Role", "")),
        "merge_fields[OFFICELOCATION1]": (None, contact.get("Office Location 1", "")),
        "merge_fields[OFFICELOCATION2]": (None, contact.get("Office Location 2", "")),
        "merge_fields[OFFICEADDRESSCOUNTRY]": (None, contact.get("Office Address - Country", "")),
        "merge_fields[ADDRESS]": (None, contact.get("Address", "")),
        "merge_fields[CITY]": (None, contact.get("City", "")),
        "merge_fields[STATECOUNTRYPROVINCE]": (None, contact.get("State/Country/Province", "")),
        "merge_fields[COUNTRY]": (None, contact.get("Country", "")),
        "merge_fields[PHONENUMBER]": (None, contact.get("Phone Number", ""))
    }

    if TEST_MODE:
        print(f"[TEST MODE] Would send to: {email} ({contact.get('First Name')} {contact.get('Last Name')}) on list {list_id}")
        return True

    response = requests.post("https://acumbamail.com/api/1/addSubscriber/", files=files)
    if response.status_code == 200:
        print(f"✅ Sent to: {email}")
        return True
    else:
        print(f"❌ Failed to add {email}. Status Code: {response.status_code}")
        print("Response:", response.text)
        return False


def process_sheet(sheet_name, batch_limit):
    sheet = gc.open(SHEET_NAME).worksheet(sheet_name)
    data = sheet.get_all_records()

    sent_count = 0
    for i, row in enumerate(data):
        if row.get("SENT:"):
            continue

        list_id = LIST_IDS.get(sheet_name)
        if not list_id:
            print(f"⚠️ List ID not found for {sheet_name}")
            break

        success = send_to_acumbamail(row, list_id)
        if success:
            sent_count += 1
            sheet.update_cell(i + 2, list(row.keys()).index("SENT:") + 1, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        if sent_count >= batch_limit:
            print(f"✅ Batch limit of {batch_limit} reached.")
            break

        time.sleep(random.uniform(1.0, 2.0))  # Basic rate limiting

    return sent_count


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    sheets_order = ["UK verified", "US verified", "UK unverified", "US unverified"]
    remaining = SEND_BATCH_LIMIT

    for sheet_name in sheets_order:
        if remaining <= 0:
            break
        print(f"\n📋 Processing sheet: {sheet_name}")
        sent = process_sheet(sheet_name, remaining)
        remaining -= sent

    print("\n✅ Email campaign run completed.")
