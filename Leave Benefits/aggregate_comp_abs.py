import duckdb
import pandas as pd
import math

#Adjusted FY to get the first entry for a given employee, position, and leave benefit
con = duckdb.connect(database='C:/Users/cb1152/Downloads/OneDrive_2025-11-03/combined_leave_data.duckdb')
df = con.execute("""
WITH with_FY AS (
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

        LEFT("Position Number", 3) AS "Agency Code",

        STRPTIME("Leave Period" || '-01', '%Y-%m-%d') AS period_date,

        a.*
    FROM all_comp_abs a
    WHERE "Time Base" != '??'
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
    "Agency Code",
    "CFIS/ORG Code",
    CBID,
    "Leave Benefit ID",
    "Leave Benefit Name",

    -- COUNT(DISTINCT UEID) AS employee_count,

    SUM(beginning_balance) AS beginning_balance,
    SUM(total_earned) AS total_earned,
    SUM(total_used) AS total_used,
    SUM(total_misc) AS total_misc,
    SUM(end_balance) AS ending_balance
                 
FROM final_calc
GROUP BY 
    FY,
    "Agency Code",
    "CFIS/ORG Code",
    CBID,
    "Leave Benefit ID",
    "Leave Benefit Name"
ORDER BY 
    FY DESC,
    "Agency Code",
    "CFIS/ORG Code",
    CBID,
    "Leave Benefit ID",
    "Leave Benefit Name";
""").df()




# ORIGINAL After it has been loaded
#con = duckdb.connect(database='C:/Users/cb1152/Downloads/OneDrive_2025-11-03/combined_leave_data.duckdb')
#df = con.execute("""
#                 WITH beginning_balances AS (
#                     SELECT distinct CONCAT(LEFT("Leave Period", 4), \'-\', TRY_CAST(LEFT("Leave Period", 4) as INT) + 1) as FY, UEID, "Position Number", "Leave Benefit ID", "Begin Balance"
#                     FROM all_comp_abs 
#                      WHERE RIGHT("Leave Period", 2) = '07'
#                  ) 
#                 , ending_balances AS (
#                     SELECT distinct CONCAT(LEFT("Leave Period", 4), \'-\', TRY_CAST(LEFT("Leave Period", 4) as INT) + 1) as FY, UEID, "Position Number", "Leave Benefit ID", "End Balance"
#                     FROM all_comp_abs 
#                      WHERE RIGHT("Leave Period", 2) = '06'
#                  ) 
#                 , pre_agg AS (
#                 SELECT *,
#                    CASE 
#                        WHEN CAST(SUBSTR("Leave Period", 6, 2) AS INTEGER) < 7 THEN
#                          CONCAT(
#                            CAST(CAST(SUBSTR("Leave Period", 1, 4) AS INTEGER) - 1 AS VARCHAR),
#                            '-',
#                            SUBSTR("Leave Period", 1, 4)
#                          )
#                        ELSE
#                          CONCAT(
#                            SUBSTR("Leave Period", 1, 4),
#                            '-',
#                            CAST(CAST(SUBSTR("Leave Period", 1, 4) AS INTEGER) + 1 AS VARCHAR)
#                          )
#                      END AS FY
#                  FROM all_comp_abs
#                 )
#                 , aggregated AS (
#                 SELECT 
#                    FY, UEID, "Department Name", "Position Number", "Leave Benefit Name", "Leave Benefit ID", "Base Pay", "Hourly Rate",
#                    SUM(CAST("Leave Earned" as DOUBLE)) as "Leave Earned",
#                    SUM(CAST("Leave Used" as DOUBLE)) as "Leave Used",
#                    SUM(CAST("Misc Amt" as DOUBLE)) as "Misc Amt",
#                    SUM(CAST("End Balance" as DOUBLE)) as "End Balance"
#                 FROM pre_agg
#                 GROUP BY FY, UEID, "Department Name", "Position Number", "Leave Benefit Name", "Leave Benefit ID", "Base Pay", "Hourly Rate"
#                 ORDER BY UEID, FY desc
#                )
#                 
#                , final AS ( 
#                  SELECT a.FY, a.UEID, a."Department Name", a."Position Number", a."Leave Benefit Name", a."Leave Benefit ID", a."Base Pay", a."Hourly Rate",
#                      bb."Begin Balance" as "Beginning Balance",
#                      a."Leave Earned",
#                      a."Leave Used",
#                      a."Misc Amt",
#                      eb."End Balance" as "Ending Balance"
#                  FROM aggregated a
#                  LEFT JOIN beginning_balances bb
#                  ON a.FY = bb.FY 
#                    AND a.UEID = bb.UEID 
#                    AND a."Position Number" = bb."Position Number" 
#                    AND a."Leave Benefit ID" = bb."Leave Benefit ID"
#                 LEFT JOIN ending_balances eb
#                  ON a.FY = eb.FY
#                    AND a.UEID = eb.UEID 
#                    AND a."Position Number" = eb."Position Number" 
#                    AND a."Leave Benefit ID" = eb."Leave Benefit ID"
#                 WHERE a.FY IN ('2024-2025')
#                  ORDER BY a.UEID, a.FY desc
#                 )
#                SELECT * FROM final
#                -- SELECT COUNT(DISTINCT (UEID, FY, "Position Number")) AS distinct_combos FROM final
#                -- SELECT UEID, FY, COUNT(DISTINCT "Position Number") AS "Number of Positions", STRING_AGG(DISTINCT "Position Number", ', ') AS "List of Positions" FROM final GROUP BY UEID, FY HAVING COUNT(DISTINCT "Position Number") > 1 ORDER BY FY, UEID
#                 """).df()
#print(df.to_string(index=False))

# ✅ Write to CSV
output_path = r"C:/Users/cb1152/Downloads/OneDrive_2025-11-03/leave_data_by_FY.csv"
base_name = output_path.rsplit('.', 1)[0]  # remove ".csv" extension

# Define your maximum rows per CSV
max_rows = 1_045_000

total_rows = len(df)
num_parts = math.ceil(total_rows / max_rows)

print(f"📊 Total rows: {total_rows:,} — splitting into {num_parts} file(s)")
for i in range(num_parts):
    start_row = i * max_rows
    end_row = min(start_row + max_rows, total_rows)
    part_df = df.iloc[start_row:end_row]

    part_filename = f"{base_name}_pt{i+1}.csv"
    part_df.to_csv(part_filename, index=False)

    print(f"✅ Saved {part_filename} — rows {start_row+1:,}–{end_row:,} ({len(part_df):,} rows)")

print("🎉 All parts saved successfully!")

