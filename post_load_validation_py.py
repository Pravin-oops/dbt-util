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