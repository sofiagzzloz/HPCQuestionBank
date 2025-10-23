import os
import re
import pandas as pd

# ============= CONFIG =============
QUESTIONS_DIR = "questions"
GOOGLE_SHEET_NAME = "HPC Practice Midterm Master"  # must match your Google Sheet name exactly
# ==================================


def parse_question_file(filepath):
    """Parse one text file and return a DataFrame of questions and options."""
    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()

    # Split based on Blackboard-style "Question X ... Question X"
    question_blocks = re.split(r"Question\s+\d+\s+1\s+Point\s+Question\s+\d+", text)
    data = []

    for block in question_blocks:
        block = block.strip()
        if not block or "Option" not in block:
            continue

        # Extract question text (before Option A)
        parts = re.split(r"Option A", block)
        question_text = parts[0].strip()

        # Extract answer options (A–E)
        options = re.findall(r"Option [A-E]\s*(.*?)\s*(?=Option [A-E]|$)", block, re.DOTALL)
        options = [opt.strip() for opt in options]
        while len(options) < 5:
            options.append("")

        data.append([question_text] + options[:5])

    # Use file name as source label
    df = pd.DataFrame(data, columns=["Question", "Option A", "Option B", "Option C", "Option D", "Option E"])
    df["Source File"] = os.path.basename(filepath)
    return df


def push_to_google_sheets(df):
    """Upload parsed data to Google Sheets."""
    import gspread
    from gspread_dataframe import set_with_dataframe
    from google.oauth2.service_account import Credentials

    creds = Credentials.from_service_account_file("credentials.json", scopes=[
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ])
    gc = gspread.authorize(creds)

    # Open your sheet and clear existing content
    sheet = gc.open(GOOGLE_SHEET_NAME).sheet1
    sheet.clear()

    # Write the new data
    set_with_dataframe(sheet, df)
    print("✅ Google Sheet updated successfully!")
    print(f"📄 Sheet name: {GOOGLE_SHEET_NAME}")
    print(f"📊 Total questions uploaded: {len(df)}")


def main():
    """Main pipeline: parse all question files and push to Google Sheets."""
    all_files = [
        os.path.join(QUESTIONS_DIR, f)
        for f in os.listdir(QUESTIONS_DIR)
        if f.endswith(".txt")
    ]

    if not all_files:
        print("⚠️ No .txt files found in 'questions/' folder.")
        return

    print(f"📂 Parsing {len(all_files)} files...")

    # Parse all .txt files
    all_dfs = [parse_question_file(f) for f in all_files]
    combined = pd.concat(all_dfs, ignore_index=True)

    # Flag duplicates
    combined["Duplicate?"] = combined.duplicated(subset=["Question"], keep="first")

    # Push to Google Sheets only
    push_to_google_sheets(combined)


if __name__ == "__main__":
    main()
