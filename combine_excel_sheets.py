import pandas as pd
from pathlib import Path

# Path to your existing workbook that has multiple tabs (from your previous extraction)
source_path = Path("extracted_budget_tables.xlsx")

# Path for the combined output workbook
output_path = Path("Combined PDF.xlsx")

# Load all sheet names
xls = pd.ExcelFile(source_path)
print(f"Found {len(xls.sheet_names)} sheets: {xls.sheet_names}")

combined_df = pd.DataFrame()

for sheet_name in xls.sheet_names:
    print(f"Reading sheet: {sheet_name}")
    df = pd.read_excel(source_path, sheet_name=sheet_name, header=None)
    
    # Add a column to identify which sheet the data came from (optional)
    df.insert(0, "SourceSheet", sheet_name)
    
    combined_df = pd.concat([combined_df, df], ignore_index=True)

# Write to single-sheet workbook
with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    combined_df.to_excel(writer, index=False, header=False, sheet_name="Combined PDF")

print(f"✅ Combined workbook created: {output_path.resolve()}")
