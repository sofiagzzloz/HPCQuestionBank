import os
import re
from typing import List

import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

# ============= CONFIG =============
QUESTIONS_DIR = "questions"
GOOGLE_SHEET_NAME = "HPC Practice Midterm Master"
CREDENTIALS_FILE = "credentials.json"

OPTION_COLUMNS = [f"Option {chr(ord('A') + i)}" for i in range(5)]
IMAGE_LINE_RE = re.compile(r".+\.(?:png|jpg|jpeg|gif|bmp)$", re.IGNORECASE)


def _normalise_space(text: str) -> str:
    """Collapse inner whitespace and strip the string."""
    return re.sub(r"\s+", " ", text).strip()


def _clean_line(line: str) -> str:
    """Return a stripped line without surrounding whitespace."""
    return line.strip().replace("\u00a0", " ")


def parse_question_block(block: str, filename: str) -> List[List[str]]:
    """Parse a single block of text into question/option rows."""

    lines = block.splitlines()
    question_lines: List[str] = []
    options: List[str] = []
    current_option: List[str] = []
    correct_indices: List[int] = []

    question_complete = False
    saw_question_prompt = False
    expect_continuation = False
    mark_next_correct = False
    skip_correct_answers_section = False

    percent_re = re.compile(r"^\d+(?:\.\d+)?%$")

    def flush_option():
        nonlocal mark_next_correct
        if current_option:
            option_text = _normalise_space(" ".join(current_option))
            if option_text:
                options.append(option_text)
                if mark_next_correct:
                    correct_indices.append(len(options) - 1)
        current_option.clear()
        mark_next_correct = False

    for raw_line in lines:
        line = _clean_line(raw_line)

        if not line:
            if question_complete:
                flush_option()
            elif saw_question_prompt and not expect_continuation:
                question_complete = True
            continue

        lower = line.lower()

        # Discard structural / grading metadata.
        if lower.startswith("question "):
            continue
        if re.fullmatch(r"\d+", line):
            continue
        if lower in {"multiple choice", "true", "false", "short answer"}:
            continue
        if lower in {"correct", "incorrect"}:
            continue
        if lower in {"/", "points possible"}:
            continue
        if lower.startswith("grade:"):
            continue
        if percent_re.match(line):
            continue
        if lower.endswith("points possible"):
            continue

        if lower.startswith("feedback"):
            flush_option()
            break

        if lower.startswith("correct answers"):
            skip_correct_answers_section = True
            flush_option()
            continue
        if skip_correct_answers_section:
            continue

        if lower.startswith("explanation"):
            skip_correct_answers_section = False
            flush_option()
            continue

        if lower.startswith("correct answer"):
            flush_option()
            if options:
                correct_indices.append(len(options) - 1)
            continue

        if lower.startswith("correct:"):
            flush_option()
            mark_next_correct = True
            question_complete = True
            continue

        if lower.startswith("incorrect:"):
            flush_option()
            mark_next_correct = False
            question_complete = True
            continue

        if lower.startswith("correct!") or lower.startswith("incorrect!"):
            flush_option()
            continue

        if not question_complete:
            question_lines.append(line)
            if re.search(r"[?!]$", line) or line.endswith(".)") or line.endswith("?)") or line.endswith("."):
                saw_question_prompt = True
            expect_continuation = line.endswith(":")
            continue

        if not options and not current_option and IMAGE_LINE_RE.match(line):
            question_lines.append(line)
            continue

        current_option.append(line)

    flush_option()

    if not question_lines or not options:
        return []

    while len(options) < len(OPTION_COLUMNS):
        options.append("")

    correct_letters = ",".join(
        sorted({chr(ord("A") + idx) for idx in correct_indices if idx < len(OPTION_COLUMNS)})
    )

    return [[_normalise_space(" ".join(question_lines))] + options[: len(OPTION_COLUMNS)] + [correct_letters, filename]]
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

    blocks = re.split(r"\n(?=Question\s*\d+\b)", text)
    filename = os.path.basename(filepath)
    rows: List[List[str]] = []

    for block in blocks:
        block = block.strip()
        if not block:
            continue
        rows.extend(parse_question_block(block, filename))

    if not rows:
        print(f"⚠️ No valid questions found in {filename}")
        return pd.DataFrame(columns=[
            "Question", *OPTION_COLUMNS, "Correct", "Source File"
        ])

    print(f"✅ Parsed {len(rows)} questions from {filename}")
    return pd.DataFrame(rows, columns=[
        "Question", *OPTION_COLUMNS, "Correct", "Source File"
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

    # Flag duplicate questions across all sources.
    normalized_question = (
        combined["Question"].str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
    )
    counts = normalized_question.value_counts()
    combined["Unique"] = normalized_question.map(lambda x: counts.get(x, 0) == 1)

    combined = truncate_cells(combined)
    push_to_google_sheets(combined)

if __name__ == "__main__":
    main()
