import pandas as pd
import re

def process_lessee_JE(file_path, sheet_name):
    """
    Reads an Excel file, skips the first 8 rows, performs transformations, and returns a cleaned DataFrame.
    """
    try:
        # Read the Excel file, skipping the first 8 rows
        df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=8, engine='openpyxl')

        # Rename columns (dynamic renaming based on Alteryx logic)
        df.columns = [re.sub(r"_", " ", str(col)) for col in df.columns]

        # Extract Debit/Credit information using regex
        if "Debit/Credit" in df.columns:
            df["D/C"] = df["Debit/Credit"].apply(lambda x: "D" if "Dr" in str(x) else "C" if "Cr" in str(x) else "ERROR")

        # Compute Amount column (if null, flip sign on another column)
        if "Amount" in df.columns and "11" in df.columns:
            df["Amount"] = df.apply(lambda row: row["11"] * -1 if pd.isnull(row["Amount"]) else row["Amount"], axis=1)

        # Fill missing "JE Title" values based on the row above (Multi-Row Formula equivalent)
        if "JE Title" in df.columns:
            df["JE Title"] = df["JE Title"].fillna(method='ffill')

        # Extract account information using regex
        df["Account Number"] = df["Debit/Credit"].apply(lambda x: re.search(r"\d+", str(x)).group() if re.search(r"\d+", str(x)) else "XXXX")
        df["Account Name"] = df["Debit/Credit"].apply(lambda x: re.sub(r"\d+", "", str(x)).strip() if x else "")

        # Final cleanup: selecting relevant columns
        selected_columns = ["JE Title", "Amount", "D/C", "Account Number", "Account Name"]
        df = df[selected_columns]

        return df

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

# Example usage
# file_path = "example.xlsx"
# sheet_name = "LS-Fund 0890 Journal Entries"
# df_cleaned = process_excel_file(file_path, sheet_name)
# 
# if df_cleaned is not None:
#     print(df_cleaned.head())
