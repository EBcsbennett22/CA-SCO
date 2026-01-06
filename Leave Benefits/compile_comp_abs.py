import os
import pandas as pd

# ---- CONFIG ----
FOLDER_PATH = r"C:\Users\cb1152\Downloads\OneDrive_2025-11-03\Leave Data - CLAS"
OUTPUT_FILE = r"C:\Users\cb1152\Downloads\OneDrive_2025-11-03\combined_leave_data.csv"
AUDIT_FILE = r"C:\Users\cb1152\Downloads\OneDrive_2025-11-03\leave_data_audit.csv"

# ---- SCRIPT ----
all_dfs = []
audit_rows = []   # store audit info here

for file in os.listdir(FOLDER_PATH):
    if file.endswith(".xlsx"):
        file_path = os.path.join(FOLDER_PATH, file)
        print(f"📘 Reading {file_path}...")

        # Read Excel
        df = pd.read_excel(file_path, header=5)

        # Count BEFORE cleaning
        original_count = len(df)

        # Drop fully empty rows
        df = df.dropna(how="all")

        # Count AFTER cleaning
        imported_count = len(df)

        # Add source file for traceability
        df["source_file"] = file

        all_dfs.append(df)

        # Save audit data
        audit_rows.append({
            "file_name": file,
            "original_row_count": original_count,
            "imported_row_count": imported_count
        })

# Combine and export
if all_dfs:
    combined_df = pd.concat(all_dfs, ignore_index=True, join='outer')
    print(f"✅ Combined {len(all_dfs)} files with {len(combined_df)} rows and {len(combined_df.columns)} columns.")

    # Write main combined CSV
    combined_df.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Combined CSV saved to: {OUTPUT_FILE}")

    # Write audit CSV
    audit_df = pd.DataFrame(audit_rows)
    audit_df.to_csv(AUDIT_FILE, index=False)
    print(f"🧾 Audit file saved to: {AUDIT_FILE}")

else:
    print("⚠️ No .xlsx files found in the folder.")
