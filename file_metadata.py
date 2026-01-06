import csv

#csv_path = r"C:/Users/cb1152/Downloads/OneDrive_2025-11-03/Combined Data/combined_leave_data.csv"
#
#with open(csv_path, "r", encoding="utf-8", newline="") as f:
#    reader = csv.reader(f)
#    header = next(reader)  # Read only the first row
#    print("Number of columns:", len(header))
#    print("Headers:")
#    for col in header:
#        print(col)


### Locate which rows are causing the insert issue ### 
csv_path = r"C:/Users/cb1152/Downloads/OneDrive_2025-11-03/Combined Data/cleaned_leave_data.csv"

EXPECTED_COMMAS = 17  # 18 columns
MAX_SOURCE_FILE_LEN = 255
MAX_PREVIEW = 20

bad_preview_count = 0

with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
    header = f.readline()  # skip header
    for line_num, line in enumerate(f, start=2):
        if bad_preview_count >= MAX_PREVIEW:
            break

        comma_count = line.count(",")
        problems = []

        # Column count check
        if comma_count != EXPECTED_COMMAS:
            problems.append(f"Wrong number of columns ({comma_count} commas)")

        # Check last column length
        last_comma = line.rfind(",")
        if last_comma != -1:
            source_part = line[last_comma+1:].strip().strip('"')
            if len(source_part) > MAX_SOURCE_FILE_LEN:
                problems.append(f"source_file too long ({len(source_part)} chars)")

        if problems:
            bad_preview_count += 1
            print(f"Row {line_num}: {'; '.join(problems)}")
            print(line.strip())
            print("-" * 80)


#import csv
#
#input_path = r"C:/Users/cb1152/Downloads/OneDrive_2025-11-03/Combined Data/combined_leave_data.csv"
#output_path = r"C:/Users/cb1152/Downloads/OneDrive_2025-11-03/Combined Data/cleaned_leave_data.csv"
#
#with open(input_path, newline='', encoding='utf-8') as infile, \
#     open(output_path, 'w', newline='', encoding='utf-8') as outfile:
#    
#    reader = csv.reader(infile, quotechar='"', delimiter=',', quoting=csv.QUOTE_MINIMAL)
#    writer = csv.writer(outfile, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
#    
#    for row in reader:
#        writer.writerow(row)