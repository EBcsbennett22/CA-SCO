import duckdb # pyright: ignore[reportMissingImports]
import pandas as pd
import math

con = duckdb.connect(database='C:/Users/cb1152/Downloads/OneDrive_2025-11-03/combined_leave_data.duckdb')
df = con.execute("""
WITH full_position_number as (
    SELECT *,
        CASE WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 12 THEN CONCAT('0', CAST("Position Number" AS VARCHAR)) 
                WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 11 THEN CONCAT('00', CAST("Position Number" AS VARCHAR)) 
                WHEN LENGTH(CAST("Position Number" AS VARCHAR)) = 10 THEN CONCAT('000', CAST("Position Number" AS VARCHAR)) 
                ELSE CAST("Position Number" AS VARCHAR) END AS "Full Position Number"                
    FROM all_comp_abs                     
)
, with_FY AS (
    SELECT
        -- Fiscal Year
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
),

-- ----------------------------
-- Totals per fiscal year
-- ----------------------------
activity AS (
    SELECT
        FY,
        UEID,
        "Leave Benefit ID",
        SUM(CAST("Leave Earned" AS DOUBLE)) AS total_earned,
        SUM(CAST("Leave Used" AS DOUBLE)) AS total_used,
        SUM(CAST("Misc Amt" AS DOUBLE)) AS total_misc
    FROM with_FY
    GROUP BY 1,2,3
),

-- ----------------------------
-- Ending balances from JUNE only
-- ----------------------------
june_endings AS (
    SELECT
        w.FY,
        w.UEID,
        w."Agency Code",
        w."CFIS/ORG Code",
        w.CBID,
        w."Leave Benefit ID",
        w."Leave Benefit Name",
        CAST(w."End Balance" AS DOUBLE) AS end_balance
    FROM with_FY w
    WHERE SUBSTR(w."Leave Period", 6, 2) = '06'
),

-- ----------------------------
-- Combine June ending balance & yearly activity
-- ----------------------------
final_calc AS (
    SELECT
        j.FY,
        j.UEID,
        j."Agency Code",
        j."CFIS/ORG Code",
        j.CBID,
        j."Leave Benefit ID",
        j."Leave Benefit Name",

        j.end_balance,
        a.total_earned,
        a.total_used,
        a.total_misc,

        -- Beginning balance calculation
        (
            j.end_balance
            - COALESCE(a.total_earned, 0)
            + COALESCE(a.total_used, 0)
            +
            CASE 
                WHEN a.total_misc < 0 THEN ABS(a.total_misc)
                ELSE -COALESCE(a.total_misc, 0)
            END
        ) AS beginning_balance
    FROM june_endings j
    LEFT JOIN activity a
        ON j.FY = a.FY
        AND j.UEID = a.UEID
        AND j."Leave Benefit ID" = a."Leave Benefit ID"
)

-- ----------------------------
-- FINAL SUMMARY
-- ----------------------------
SELECT
    FY,
    -- "Agency Code",
    LPAD(CAST("CFIS/ORG Code" AS VARCHAR), 4, '0') AS "CFIS/ORG Code",
    CBID,
    "Leave Benefit ID",
    "Leave Benefit Name",

    -- COUNT(DISTINCT UEID) AS employee_count,

    -- SUM(beginning_balance) AS beginning_balance,
    -- SUM(total_earned) AS total_earned,
    -- SUM(total_used) AS total_used,
    -- SUM(total_misc) AS total_misc,
    ROUND(SUM(end_balance) * 4) / 4 AS ending_balance
                 
FROM final_calc
GROUP BY 
    FY,
    -- "Agency Code",
    "CFIS/ORG Code",
    CBID,
    "Leave Benefit ID",
    "Leave Benefit Name"
ORDER BY 
    FY DESC,
    -- "Agency Code",
    "CFIS/ORG Code",
    CBID,
    "Leave Benefit ID",
    "Leave Benefit Name";
""").df()

# ✅ Write to CSV
output_path = r"C:/Users/cb1152/Downloads/OneDrive_2025-11-03/leave_balance_by_agency.csv"
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

