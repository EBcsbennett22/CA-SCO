#!pip install pdfplumber camelot-py[cv] xlsxwriter pandas

import os
import camelot
import matplotlib.pyplot as plt
import pdfplumber
import itertools
from operator import itemgetter
from time import sleep
from openai import OpenAI
import pandas as pd
import json

# ---- CONFIG ----

PDF_PATH = "/content/drive/MyDrive/CASCO_PDFs"  # change to your mounted drive or folder in Colab
OPENAI_API_KEY=''
FLAVOR = "stream"  # or "lattice"
EDGE_TOL = 500     # for stream mode tuning
client = OpenAI(api_key=OPENAI_API_KEY)

def fetch_report_details_with_gpt(page_text: str, page_no: int, agency: str):
    prompt = f"""
    You are a professional data extractor and compilor and a budget analyst with knowledge of California State Budgets.
    From the text below, extract a JSON object with the following fields:
    - agency: Usually in the second line of the complete text, formatted like '0855 California Gambling Control Commission' With the agency number first followed by the name of the agency. If that is not provided at the top of the page that is being processed (again, usually in that second line), then use {agency}
    - page: the report page the data is on. For this batch, the page is {page_no}
    - rows: a list of objects that contain the columns for the key data in the text body. Ignore any of the headers in the page like 'LEGISLATIVE, JUDICIAL, AND EXECUTIVE' or '2025-26 GOVERNOR'S BUDGET — LJE 1' as well as the agency name and number. Also ignore information that is at the base of a page and contains mostly text like '† Savings resulting from SEC. 4.05 and/or SEC. 4.12 of the 2024 Budget Act are currently being recorded as an unallocated
        statewide set-aside. As a result, this department’s budgetary displays may reflect overstated expenditures and may also potentially reflect negative fund balances in particular programs and funds.' or '* Dollars in thousands, except in Salary Range. Numbers may not add or match to other statements due to rounding of budget details'. These rows should primary be comprised of
        four columns, the first being the row detail and the next three for each fiscal year for the appropriations and adjustments (2023-24* 2024-25* 2025-26* ). There should also be rows for the fund names, but there will not be financial data as these will serve as headers. Also include rows for the totals, which you will not have to calculate since they are on the sheet.

    Please format the numerical values in columns 2,3 and 4 in dollars $ with negative values enclosed in parenthesis ()
    Please aggregate the rows for ALL PAGES in the report into the output JSON. There are also details about which page in a report the data comes from, please ignore that data and continue with the rows on the following page, ignoring the headers where applicable.
    Return only valid JSON — no commentary, no markdown, no extra text, please trim leading/following spaces from the values.
    Example of the expected format:
    {{
        "agency": "0855 California Gambling Control Commission",
        "page": 1,
        "rows": [
            {{"column_1": "DETAIL OF APPROPRIATIONS AND ADJUSTMENTS † ", "column_2": "", "column_3": "", "column_4": ""}},
            {{"column_1": "1 STATE OPERATIONS", "column_2": "2023-24*", "column_3": "2024-25*", "column_4": "2025-26*"}},
            {{"column_1": "0367 Indian Gaming Special Distribution Fund", "column_2": "", "column_3": "", "column_4": ""}},
            {{"column_1": "APPROPRIATIONS", "column_2": "", "column_3": "", "column_4": ""}},
            {{"column_1": "001 Budget Act appropriation", "column_2": "$3,540", "column_3": "$3,866", "column_4": "$3,768"}},
            {{"column_1": "Allocation for Employee Compensation", "column_2": "-", "column_3": "$66", "column_4": "-"}},
            {{"column_1": "Allocation for Staff Benefits", "column_2": "-", "column_3": "$37", "column_4": "-"}},
            {{"column_1": "8089 Tribal Nation Grant Fund ", "column_2": "", "column_3": "", "column_4": ""}}
        ]
    }}
    Text:
    {page_text}
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # cheaper + better for extraction
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        content = response.choices[0].message.content.strip()
        #parsed = extract_json_from_chatcompletion(content)
        # Skip recipes with no ingredients or steps
        #if not parsed.get("ingredients") or not parsed.get("steps"):
        #    print(f"Skipping {url} — missing ingredients or steps.")
        #    continue
        #structured_recipes.append(parsed)
        #print(f"✅ Processed {url} ({len(structured_recipes)}/{target_count})")
    except Exception as e:
        print(f"Error processing page: {e}")

    sleep(1)  # polite rate limit

    return content

# 1️⃣ Setup helper functions
def group_by_line(words, y_tolerance=3):
    """Group words that are on approximately the same horizontal line."""
    lines = []
    sorted_words = sorted(words, key=lambda w: w["top"])
    for _, group in itertools.groupby(sorted_words, key=lambda w: round(w["top"] / y_tolerance)):
        lines.append(list(group))
    return lines

def create_pseudo_table(words):
    lines = group_by_line(words)
    table_str = []
    for line in lines:
        line = sorted(line, key=itemgetter("x0"))
        row_str = ""
        prev_x = 0
        for w in line:
            space_count = int((w["x0"] - prev_x) / 10)  # crude spacing approximation
            row_str += " " * max(space_count, 1) + w["text"]
            prev_x = w["x1"]
        table_str.append(row_str)
    return "\n".join(table_str)

def results_to_dataframe(page_results_list):
    """Convert list of GPT JSON results into a structured DataFrame."""
    all_rows = []
    for page_result in page_results_list:
        agency = page_result.get("agency", "").strip()
        rows = page_result.get("rows", [])

        # Add header row once per file
        all_rows.append({
            "column_1": agency,
            "column_2": "",
            "column_3": "",
            "column_4": ""
        })
        all_rows.extend(rows)

        # Blank line separator between files
        all_rows.append({"column_1": "", "column_2": "", "column_3": "", "column_4": ""})

    return pd.DataFrame(all_rows)

# ---- MAIN EXECUTION ----
if __name__ == "__main__":
    all_page_results = []

    pdf_files = [f for f in os.listdir(PDF_PATH) if f.lower().endswith(".pdf")]
    print(f"📂 Found {len(pdf_files)} PDFs to process.\n")

    for pdf_file in pdf_files:
        pdf_path = os.path.join(PDF_PATH, pdf_file)
        print(f"\n📘 Processing file: {pdf_file}")

        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            print(f"   → Found {total_pages} pages.\n")

            for page_num, page in enumerate(pdf.pages, start=1):
                words = page.extract_words()
                pseudo_table_output = create_pseudo_table(words)

                agency = pdf_file.replace('.pdf', '')[-4:]

                print(f"🧠 Running AI analysis for page {page_num}...")
                openai_results = fetch_report_details_with_gpt(pseudo_table_output, page_num, agency)

                # Parse JSON if needed
                if isinstance(openai_results, str):
                    try:
                        openai_result = json.loads(openai_results)
                    except json.JSONDecodeError:
                        print(f"⚠️ Warning: Could not parse JSON for page {page_num} in {pdf_file}")
                        continue
                else:
                    openai_result = openai_results

                all_page_results.append(openai_result)
                print(f"✅ Page {page_num} analyzed.\n")

    # Combine all into one dataframe
    df = results_to_dataframe(all_page_results)
    print("\n🎯 All PDFs processed successfully!")
    #print(df.head(10))

    df.head(100)

    # Optional: Save to CSV
    output_csv = os.path.join(PDF_PATH, "combined_results.csv")
    df.to_csv(output_csv, index=False)
    print(f"\n💾 Results saved to: {output_csv}")