import os
import re
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# === CONFIG ===
QUESTIONS_FOLDER = "questions"
GOOGLE_SHEET_NAME = "HPC Practice Midterm Master"

# === AUTH ===
creds = Credentials.from_service_account_file(
    "credentials.json",
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc = gspread.authorize(creds)
sheet = gc.open(GOOGLE_SHEET_NAME).sheet1

# === PARSE ALL QUESTIONS ===
def parse_txt_files():
    all_questions = []
    pattern = r"Question\s+(\d+).*?(?=(?:Question\s+\d+)|\Z)"
    for file in os.listdir(QUESTIONS_FOLDER):
        if file.endswith(".txt"):
            with open(os.path.join(QUESTIONS_FOLDER, file), "r", encoding="utf-8") as f:
                text = f.read()
                matches = re.findall(pattern, text, flags=re.DOTALL)
                for match in matches:
                    all_questions.append(match.strip())
    return pd.DataFrame({"Question": list(set(all_questions))})  # remove duplicates

# === UPDATE GOOGLE SHEET ===
df = parse_txt_files()
set_with_dataframe(sheet, df)
print(f"✅ Updated Google Sheet with {len(df)} unique questions.")

