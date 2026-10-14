# Databricks notebook source

from datetime import datetime

# COMMAND ----------

# =================================================================================
# WIDGETS
# =================================================================================

dbutils.widgets.text("table_names", "")
dbutils.widgets.text("min_row_count", "1")

table_names = dbutils.widgets.get("table_names")
min_row_count = int(dbutils.widgets.get("min_row_count"))

dbutils.widgets.text("segment_column", "customer_segment")
dbutils.widgets.text("allowed_segments", "UKC,UK-ICB")

segment_column = dbutils.widgets.get("segment_column").strip()
allowed_segments = [
    value.strip()
    for value in dbutils.widgets.get("allowed_segments").split(",")
    if value.strip()
]

# COMMAND ----------


# =================================================================================
# PARAMETER VALIDATION
# =================================================================================

if not table_names:
    raise Exception(
        "Mandatory parameter 'table_names' is missing"
    )

tables = [
    table.strip()
    for table in table_names.split(",")
    if table.strip()
]

# COMMAND ----------

# =================================================================================
# LOGGER FUNCTIONS
# =================================================================================

def timestamp():
    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

def log(message, level="INFO"):
    print(
        f"[{timestamp()}] - "
        f"[{level}] "
        f"{message}"
    )

def print_header(message):
    print("\n" + "=" * 80)
    print(message)
    print("=" * 80 + "\n")

def print_separator():
    print("-" * 80)

# COMMAND ----------

# =================================================================================
# PYDEEQU SEGMENT CHECK
# =================================================================================

import os

os.environ["SPARK_VERSION"] = spark.version[:3]

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult


def check_allowed_segments(df, column, allowed):
    """Return a list of failure messages. Empty list means the check passed."""

    if column not in df.columns:
        return [f"column '{column}' not found"]

    check = (
        Check(spark, CheckLevel.Error, "allowed segment check")
        .isComplete(column)
        .isContainedIn(column, allowed)
    )

    result = (
        VerificationSuite(spark)
        .onData(df)
        .addCheck(check)
        .run()
    )

    rows = (
        VerificationResult
        .checkResultsAsDataFrame(spark, result)
        .collect()
    )

    return [
        f"{r['constraint']} : {r['constraint_message']}"
        for r in rows
        if r["constraint_status"] != "Success"
    ]

# COMMAND ----------


# =================================================================================
# START VALIDATION
# =================================================================================

print_header(
    f"STARTING VALIDATION FOR {len(tables)} TABLE(S)"
)

validation_summary = []
validation_errors = []

# COMMAND ----------

# =================================================================================
# PROCESS TABLES
# =================================================================================

for idx, table_name in enumerate(tables, start=1):

    print_separator()

    print(
        f"TABLE {idx}/{len(tables)}: "
        f"{table_name}"
    )

    print_separator()

    try:

        # -------------------------------------------------------------------------
        # CHECK TABLE EXISTENCE
        # -------------------------------------------------------------------------

        log("Checking if table exists...")

        if not spark.catalog.tableExists(table_name):

            log(
                "Table does not exist",
                "ERROR"
            )

            validation_errors.append(
                f"{table_name} : TABLE NOT FOUND"
            )

            validation_summary.append(
                {
                    "table": table_name,
                    "status": "FAILED",
                    "reason": "TABLE NOT FOUND"
                }
            )

            print()

            continue

        log("Table exists")

        # -------------------------------------------------------------------------
        # ROW COUNT
        # -------------------------------------------------------------------------

        log("Counting rows...")

        row_count = (
            spark.table(table_name)
            .count()
        )

        print(
            f"  Row Count : "
            f"{row_count:,}"
        )

        # -------------------------------------------------------------------------
        # VALIDATION
        # -------------------------------------------------------------------------

        if row_count < min_row_count:

            log(
                "Validation Failed",
                "ERROR"
            )

            validation_errors.append(
                f"{table_name} : Expected >= "
                f"{min_row_count}, Actual={row_count}"
            )

            validation_summary.append(
                {
                    "table": table_name,
                    "status": "FAILED",
                    "reason": f"ROW COUNT={row_count}"
                }
            )

        else:

            log(
                f"Validation Passed "
                f"(Row Count={row_count:,})"
            )

            validation_summary.append(
                {
                    "table": table_name,
                    "status": "PASSED",
                    "reason": (
                        f"ROW COUNT={row_count:,}"
                    )
                }
            )

        # -------------------------------------------------------------------------
        # ALLOWED SEGMENT CHECK
        # -------------------------------------------------------------------------

        if segment_column and allowed_segments:

            log(
                f"Checking {segment_column} "
                f"in {allowed_segments}..."
            )

            segment_failures = check_allowed_segments(
                spark.table(table_name),
                segment_column,
                allowed_segments
            )

            if segment_failures:

                log(
                    "Segment check failed",
                    "ERROR"
                )

                for msg in segment_failures:
                    validation_errors.append(
                        f"{table_name} : {msg}"
                    )

                validation_summary.append(
                    {
                        "table": table_name,
                        "status": "FAILED",
                        "reason": "INVALID SEGMENT VALUES"
                    }
                )

            else:

                log("Segment check passed")

                validation_summary.append(
                    {
                        "table": table_name,
                        "status": "PASSED",
                        "reason": (
                            f"SEGMENT IN {allowed_segments}"
                        )
                    }
                )

    except Exception as ex:

        log(
            f"Exception occurred : {str(ex)}",
            "ERROR"
        )

        validation_errors.append(
            f"{table_name} : {str(ex)}"
        )

        validation_summary.append(
            {
                "table": table_name,
                "status": "FAILED",
                "reason": str(ex)
            }
        )

    print()

# COMMAND ----------


# =================================================================================
# SUMMARY
# =================================================================================

print_header("VALIDATION SUMMARY")

for result in validation_summary:

    icon = (
        "PASS"
        if result["status"] == "PASSED"
        else "FAIL"
    )

    print(
        f"{icon:<5} "
        f"{result['table']:<70} "
        f"{result['reason']}"
    )

print()

total_tables = len(validation_summary)

passed_count = len(
    [
        x
        for x in validation_summary
        if x["status"] == "PASSED"
    ]
)

failed_count = len(
    [
        x
        for x in validation_summary
        if x["status"] == "FAILED"
    ]
)

print("-" * 80)

print(f"Total Tables : {total_tables}")
print(f"Passed       : {passed_count}")
print(f"Failed       : {failed_count}")

print("-" * 80)

# COMMAND ----------


# =================================================================================
# FAIL WORKFLOW
# =================================================================================

if validation_errors:

    failure_message = "\n".join(
        validation_errors
    )

    print_header(
        "POST LOAD VALIDATION FAILED"
    )

    print(failure_message)

    raise Exception(
        f"""
POST LOAD VALIDATION FAILED

Validation Errors:

{failure_message}
"""
    )

# COMMAND ----------

# =================================================================================
# SUCCESS
# =================================================================================

summary_lines = []

for result in validation_summary:

    summary_lines.append(
        f"{result['table']} --> "
        f"{result['status']} --> "
        f"{result['reason']}"
    )

success_message = f"""
POST LOAD VALIDATION SUCCESSFUL

Total Tables : {total_tables}
Passed       : {passed_count}
Failed       : {failed_count}

================================================================================

{chr(10).join(summary_lines)}

================================================================================
"""

print_header(
    "POST LOAD VALIDATION SUCCESSFUL"
)

print(success_message)

dbutils.notebook.exit(
    success_message
)