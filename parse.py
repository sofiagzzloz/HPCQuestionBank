import os
import re
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# ============= CONFIG =============
QUESTIONS_DIR = "questions"
GOOGLE_SHEET_NAME = "HPC Practice Midterm Master"
CREDENTIALS_FILE = "credentials.json"
# ==================================

def truncate_cells(df, max_len=49000):
    """Truncate to fit Google Sheets limits."""
    for c in df.columns:
        df[c] = df[c].astype(str).apply(lambda x: x[:max_len])
    return df

# ----------------------------------------------------------
# 🧠 MUCH BETTER PARSER
# ----------------------------------------------------------
def parse_question_file(filepath):
    text = open(filepath, "r", encoding="utf-8", errors="ignore").read()
    text = text.replace("\r", "\n")

    # Split by "Question", number, or blank lines followed by options
    blocks = re.split(r"(?:^|\n)(?:Question\s*\d+[\.:]?|^\d+\.)", text, flags=re.MULTILINE)
    blocks = [b.strip() for b in blocks if b.strip()]
    data = []

    for block in blocks:
        # find question line
        question_match = re.search(r"^(.*?)(?=\n[A-E][\.\)]|\n\(A\)|\nA\s|\nOption A|\nA\)|\nA\.)", block, re.S | re.M)
        question = question_match.group(1).strip() if question_match else block.splitlines()[0].strip()

        # find all options (A–E) with any punctuation
        opts = re.findall(
            r"(?:^|\n)\(?([A-Ea-e])[\)\.\s:-]+\s*(.+?)(?=\n\(?[A-Ea-e][\)\.\s:-]+|\Z)",
            block,
            re.S,
        )
        options = [""] * 5
        for label, textopt in opts:
            idx = ord(label.upper()) - ord("A")
            if 0 <= idx < 5:
                options[idx] = " ".join(textopt.split())

        # detect correct answer (several possible markers)
        correct = ""
        corr_match = re.search(r"(?i)(correct answer|answer|correct|right)[:\s-]*([A-Ea-e])", block)
        if corr_match:
            correct = corr_match.group(2).upper()
        else:
            # if * marks correct
            star = re.search(r"\*+\s*\(?([A-Ea-e])[\)\.\s:-]", block)
            if star:
                correct = star.group(1).upper()

        # pad options
        while len(options) < 5:
            options.append("")

        data.append([question] + options[:5] + [correct, os.path.basename(filepath)])

    if not data:
        print(f"⚠️ No valid questions found in {os.path.basename(filepath)}")
    else:
        print(f"✅ Parsed {len(data)} questions from {os.path.basename(filepath)}")

    return pd.DataFrame(data, columns=[
        "Question", "Option A", "Option B", "Option C", "Option D", "Option E", "Correct", "Source File"
    ])

# ----------------------------------------------------------
# 🔗 Upload to Google Sheets
# ----------------------------------------------------------
def push_to_google_sheets(df):
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ])
    gc = gspread.authorize(creds)
    sheet = gc.open(GOOGLE_SHEET_NAME).sheet1
    sheet.clear()
    set_with_dataframe(sheet, df)
    print(f"✅ Uploaded {len(df)} rows to Google Sheet → {GOOGLE_SHEET_NAME}")

# ----------------------------------------------------------
def main():
    files = [os.path.join(QUESTIONS_DIR, f) for f in os.listdir(QUESTIONS_DIR) if f.endswith(".txt")]
    if not files:
        print("⚠️ No .txt files found in 'questions/' folder.")
        return

    all_dfs = []
    for f in files:
        df = parse_question_file(f)
        all_dfs.append(df)
    combined = pd.concat(all_dfs, ignore_index=True)

    combined = truncate_cells(combined)
    push_to_google_sheets(combined)

if __name__ == "__main__":
    main()
