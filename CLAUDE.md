# dbt-util (dbt_shared_utility)

Shared dbt macros for batch audit and reconciliation on Databricks, plus Databricks notebooks
(e.g. `post_load_validation_py.py`). This repo is a working copy of code that runs in the
Barclays environment; when the user refers to "the Barclays side", that version is the source
of truth for content and layout.

## Working rules

- Formatting requests mean whitespace/layout only. Do not add, remove or reword comments,
  logic, strings or names. If a target line count is given, match it exactly and explain how.
- Do not add explanatory comments the user did not ask for. Keep the existing section banners.
- Match the style below in every new or edited file; do not introduce a different style.
- Table references are currently hardcoded per macro. Do not change catalog/schema names
  or switch them to vars unless asked; point out inconsistencies instead.
- `audit_batch_run` is append-only (event rows written by `batch_run_event`).
  `batch_run_update` is retired and raises on use; never reintroduce updates/merges on it.

## Jinja / SQL macro style

- One public macro per file, file named after the macro (`batch_run_event.sql`);
  small shared helpers live together in `batch_helpers.sql`.
- 4-space indentation per block level. The macro body is indented inside
  `{% macro %}` ... `{% endmacro %}`.
- Start runtime macros with the execute guard:

  ```jinja
  {% if not execute %}
      {{ return('') }}
  {% endif %}
  ```

- Multi-line `set`: value on the next line indented +4, closing `%}` aligned with `{% set`.
  One argument per line in calls:

  ```jinja
  {% set end_table_version =
      get_current_table_version(
          target_relation
      )
  %}
  ```

- `{% do ... %}` macro calls use keyword arguments, one per line, closing `) %}` aligned
  with `{% do`. Call macros unqualified (`batch_run_event(...)`, not `dbt_shared_utility.`).
- String concatenation breaks before the operator, one `~` term per line; multi-line
  conditions break before `and` / `or`.
- Section banners are three lines, uppercase title, fixed width:

  ```jinja
  {# ======================================================== #}
  {# SUCCESSFUL MODEL OR SNAPSHOT                             #}
  {# ======================================================== #}
  ```

- Event/status values are uppercase (`TASK_STARTED`, `SUCCESS`, `RECON_FAILED`).
  Event details are a dict with double-quoted keys piped through `| tojson`.
- Logging: `{{ log('KEY=' ~ value ~ ' OTHER_KEY=' ~ other, info=True) }}` with uppercase keys.
- Validation failures are appended to a `validation_failures` list as
  `table_type ~ '-' ~ model_name ~ ': message'` and raised once at the end with
  `exceptions.raise_compiler_error(... validation_failures | join(' | '))`.

### SQL inside macros

- Lowercase keywords. `select`, `from`, `where` each on their own line; subquery
  parentheses on their own lines.
- In column lists, each expression on its own line with `as alias` on the next line,
  indented +4, and a blank line between columns.
- Interpolate strings as `'{{ sql_escape(value) }}'`; typed nulls as `cast(null as bigint)`.

## Databricks notebook (Python) style

- Source format: first line `# Databricks notebook source`, cells separated by
  `# COMMAND ----------`.
- Each cell opens with an 81-character banner and an uppercase title; sub-sections use
  `# ----` banners indented with the code:

  ```python
  # =================================================================================
  # PARAMETER VALIDATION
  # =================================================================================
  ```

- Cell order: imports, WIDGETS, PARAMETER VALIDATION, LOGGER FUNCTIONS, helpers,
  processing, SUMMARY, FAIL WORKFLOW, SUCCESS.
- Inputs come from `dbutils.widgets.text` / `.get`; a missing mandatory input raises
  `Exception("Mandatory parameter '<name>' is missing")`.
- Logging through `timestamp()`, `log(message, level="INFO")`, `print_header`,
  `print_separator`.
- One argument per line in multi-line calls; long f-strings split across lines.
- Collect errors in a list, print a summary, then raise one `Exception` with all errors;
  on success end with `dbutils.notebook.exit(message)`.
