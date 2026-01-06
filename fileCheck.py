import os
import openpyxl
import csv
import shutil

# Directories
source_directory = "C:/Users/cb1152/OneDrive - Eide Bailly LLP/Current Projects/State of California/Capital Assets/Capital Asset Aggregation/Inputs/All Report 18's Excel/Report18s"
dnp_folder = "C:/Users/cb1152/OneDrive - Eide Bailly LLP/Current Projects/State of California/Capital Assets/Capital Asset Aggregation/Inputs/All Report 18's Excel/Report18s/DNP"

# Ensure the DNP folder exists
if not os.path.exists(dnp_folder):
    os.makedirs(dnp_folder)

# List to store results (file, sheet) tuples for files with no "Beginning Balance"
sheets_without_balance = []

# Loop through all .xlsx files in the source directory
for file in os.listdir(source_directory):
    # Check if file ends with .xlsx and doesn't contain "R19" or "Report 19"
    if file.endswith(".xlsx") and "R19" not in file and "Report 19" not in file:
        file_path = os.path.join(source_directory, file)
        wb = openpyxl.load_workbook(file_path, data_only=True)
        
        # Flag to determine if this file has at least one sheet missing the balance text
        file_has_missing_balance = False
        
        # Check each sheet in the workbook
        for sheet_name in wb.sheetnames:
            if "Diff" in sheet_name:
                continue

            sheet = wb[sheet_name]
            cell_value = sheet["F14"].value  # Read cell F14
            
            # Check if F14 does not contain "Beginning Balance"
            if cell_value is None or "Beginning Balance" not in str(cell_value):
                sheets_without_balance.append((file, sheet_name))
                file_has_missing_balance = True
                # Once one sheet fails the check, we move the file and skip checking further sheets.
                break

# Write the results to a CSV file
csv_filename = "sheets_without_balance1.csv"
with open(csv_filename, mode="w", newline="") as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["File", "Sheet Name"])
    for file_name, sheet_name in sheets_without_balance:
        writer.writerow([file_name, sheet_name])

print(f"Results exported to {csv_filename}")

# Create a set of unique file names to move (each file only needs to be moved once)
files_to_move = {file_name for file_name, _ in sheets_without_balance}

# Move each file in the set to the DNP folder
for file_name in files_to_move:
    src_path = os.path.join(source_directory, file_name)
    dst_path = os.path.join(dnp_folder, file_name)
    shutil.move(src_path, dst_path)
    print(f"Moved {file_name} to {dnp_folder}")
