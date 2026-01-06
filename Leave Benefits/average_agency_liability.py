import duckdb # pyright: ignore[reportMissingImports]
import pandas as pd
import math

con = duckdb.connect(database='C:/Users/cb1152/Downloads/OneDrive_2025-11-03/combined_leave_data.duckdb')
df = con.execute("""
WITH full_position_number AS (
    SELECT *,
        CASE 
            WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 12 THEN CONCAT('0', CAST("Position Number" AS VARCHAR)) 
            WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 11 THEN CONCAT('00', CAST("Position Number" AS VARCHAR)) 
            WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 10 THEN CONCAT('000', CAST("Position Number" AS VARCHAR)) 
            ELSE CAST("Position Number" AS VARCHAR)
        END AS "Full Position Number"
    FROM all_comp_abs
),

with_FY AS (
    SELECT
        CONCAT(
            CAST(
                CASE 
                    WHEN CAST(SUBSTR("Leave Period", 6, 2) AS INT) < 7 
                        THEN CAST(CAST(SUBSTR("Leave Period", 1, 4) AS INT) - 1 AS VARCHAR)
                    ELSE SUBSTR("Leave Period", 1, 4)
                END AS VARCHAR
            ),
            '-',
            CAST(
                CASE 
                    WHEN CAST(SUBSTR("Leave Period", 6, 2) AS INT) < 7 
                        THEN SUBSTR("Leave Period", 1, 4)
                    ELSE CAST(CAST(SUBSTR("Leave Period", 1, 4) AS INT) + 1 AS VARCHAR)
                END AS VARCHAR
            )
        ) AS FY,

        LEFT("Full Position Number", 3) AS "Agency Code",
        STRPTIME("Leave Period" || '-01', '%Y-%m-%d') AS period_date,

        a.*
    FROM full_position_number a
    WHERE "Time Base" != '??'
    AND LEFT("Full Position Number", 3) NOT IN (
        '001','002','003','004','007','012','019','035','063','275','276','294',
        '298','313','320','321','323','324','339','341','345','358','402','532',
        '693','801','815','823'
    )
    AND SUBSTR(a."Leave Period", 6, 2) = '06'
),
final_calc AS (
    SELECT
        j.FY,
        j.UEID,
        j."Agency Code",
        j."CFIS/ORG Code",
        j.CBID,
        j."Leave Benefit ID",
        j."Leave Benefit Name",
        j."End Balance" as end_balance,
        j."Hourly Rate" as hourly_rate
    FROM with_FY j
),
-- ------------------------------------------------
-- Compute liability at employee-benefit level
-- ------------------------------------------------
employee_liability AS (
    SELECT
        f.FY,
        f.UEID,
        f."Agency Code",
        f."CFIS/ORG Code",
        f."Leave Benefit ID",
        f.end_balance,
        f.hourly_rate,
        (CAST(f.end_balance as DOUBLE) * CAST(f.hourly_rate as DOUBLE)) AS liability
    FROM final_calc f
),

-- ------------------------------------------------
-- Sum liabilities per employee
-- ------------------------------------------------
employee_totals AS (
    SELECT
        FY,
        UEID,
        "Agency Code",
        "CFIS/ORG Code",
        SUM(CAST(end_balance as DOUBLE)) as hours,
        SUM(liability) AS total_employee_liability
    FROM employee_liability
    GROUP BY 1,2,3,4
)
                 

, stage as (
SELECT
    FY,
    -- "Agency Code",
    "CFIS/ORG Code",
    SUM(hours) AS total_hours_raw,
    ROUND(SUM(hours) * 4) / 4 AS total_hours,
    -- ROUND(AVG(total_employee_liability), 2) AS avg_outstanding_liability
    SUM(total_employee_liability) AS total_outstanding_liability

FROM employee_totals
-- WHERE "Agency Code" = '051' AND FY = '2024-2025'
GROUP BY 1,2 --,3
ORDER BY FY DESC, CAST("CFIS/ORG Code" AS INTEGER) ASC -- "Agency Code",
)
                 
SELECT 
    FY,
    -- "Agency Code",
    LPAD(CAST("CFIS/ORG Code" AS VARCHAR), 4, '0') AS "CFIS/ORG Code",
    -- avg_outstanding_liability,
    total_hours,
    ROUND(total_outstanding_liability / NULLIF(total_hours_raw,0), 2) AS weighted_avg_hourly_rate  
FROM stage    
-- SELECT * FROM employee_totals WHERE "Agency Code" = '051' AND UEID = '100021385' AND FY = '2024-2025' LIMIT 10       
""").df()


print(df.to_string(index=False))

# ✅ Write to CSV

output_path = r"C:/Users/cb1152/Downloads/OneDrive_2025-11-03/average_agency_liability.csv"
base_name, ext = output_path.rsplit('.', 1)  # split filename & extension

# Define your maximum rows per CSV
max_rows = 1_045_000

total_rows = len(df)
num_parts = math.ceil(total_rows / max_rows)

print(f"📊 Total rows: {total_rows:,} — splitting into {num_parts} file(s)")

# Handle empty dataframe gracefully
if total_rows == 0:
    # Save an empty file with headers to the original path
    df.to_csv(output_path, index=False)
    print(f"⚠️ Dataframe is empty. Saved an empty file with headers to: {output_path}")
else:
    for i in range(num_parts):
        start_row = i * max_rows
        end_row = min(start_row + max_rows, total_rows)
        part_df = df.iloc[start_row:end_row]

        # Only append _ptX when there are multiple parts
        if num_parts == 1:
            part_filename = output_path
        else:
            part_filename = f"{base_name}_pt{i+1}.{ext}"

        part_df.to_csv(part_filename, index=False)

        print(f"✅ Saved {part_filename} — rows {start_row+1:,}–{end_row:,} ({len(part_df):,} rows)")

    print("🎉 All parts saved successfully!")
