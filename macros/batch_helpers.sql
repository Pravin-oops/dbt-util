{% macro get_databricks_job_run_id() %}
  {% set run_id = var(
    'databricks_job_run_id',
    invocation_id
  ) %}

  {{ return(run_id | string) }}
{% endmacro %}

{% macro get_databricks_job_name() %}
  {% set job_name = var(
    'databricks_job_name',
    'LOCAL_DBT_RUN'
  ) %}

  {{ return(job_name | string) }}
{% endmacro %}

{% macro get_databricks_job_start_ts() %}
  {% set start_ts = var(
    'databricks_job_start_ts',
    ''
  ) %}

  {{ return(start_ts | string) }}
{% endmacro %}

{% macro sql_escape(value) %}
  {% if value is none %}
    {{ return('') }}
  {% endif %}

  {{ return(
    (value | string).replace("'", "''")
  ) }}
{% endmacro %}

{% macro apply_clustering(cluster_col) %}
  ALTER TABLE {{ this }}
  CLUSTER BY ({{ cluster_col }})
{% endmacro %}

{# ========================================================================= #}
{# RETURN LATEST TABLE VERSION                                               #}
{# Used for successful models                                                #}
{# ========================================================================= #}

{% macro get_latest_table_version(target_relation) %}

  {% if not execute %}
    {{ return(none) }}
  {% endif %}

  {% if target_relation is none
      or target_relation | string | trim | length == 0 %}
    {{ return(none) }}
  {% endif %}

  {% set version_sql %}
    select
      cast(version as bigint) as table_version
    from
      (
        describe history {{ target_relation }}
      )
    order by version desc
    limit 1
  {% endset %}

  {% set version_result = run_query(version_sql) %}

  {% if version_result is none
      or version_result.columns | length == 0
      or version_result.columns[0].values() | length == 0 %}
    {{ return(none) }}
  {% endif %}

  {% set version_value =
    version_result.columns[0].values()[0]
  %}

  {% if version_value is none %}
    {{ return(none) }}
  {% endif %}

  {{ return(version_value | int) }}

{% endmacro %}

{# ========================================================================= #}
{# RETURN SNAPSHOT METRICS AND SNAPSHOT TABLE VERSION                        #}
{# ========================================================================= #}

{% macro get_snapshot_audit_details(target_relation) %}

  {% set empty_result = {
    'history_found': false,
    'table_version': none,
    'total_rows_inserted_updated': 0,
    'rows_inserted': 0,
    'rows_updated': 0,
    'snapshot_metrics': none
  } %}

  {% if not execute %}
    {{ return(empty_result) }}
  {% endif %}

  {% if target_relation is none
      or target_relation | string | trim | length == 0 %}
    {{ return(empty_result) }}
  {% endif %}

  {#
    Select the latest snapshot write.

    MERGE is used for normal dbt snapshot processing.

    CREATE operations are included for the first execution,
    where the snapshot target table may be created.
  #}

  {% set snapshot_history_sql %}
    select
      cast(version as bigint)
        as table_version,

      cast(
        coalesce(
          operationMetrics['numOutputRows'],
          operationMetrics['numTargetRowsInserted'],
          '0'
        ) as bigint
      ) as total_rows_inserted_updated,

      case
        when upper(operation) in
        (
          'CREATE TABLE AS SELECT',
          'CREATE OR REPLACE TABLE AS SELECT'
        )
        then cast(
          coalesce(
            operationMetrics['numOutputRows'],
            '0'
          ) as bigint
        )

        else cast(
          coalesce(
            operationMetrics['numTargetRowsInserted'],
            '0'
          ) as bigint
        )
      end as rows_inserted,

      cast(
        coalesce(
          operationMetrics['numTargetRowsMatchedUpdated'],
          operationMetrics['numTargetRowsUpdated'],
          '0'
        ) as bigint
      ) as rows_updated

    from
      (
        describe history {{ target_relation }}
      )

    where upper(operation) in
      (
        'MERGE',
        'CREATE TABLE AS SELECT',
        'CREATE OR REPLACE TABLE AS SELECT'
      )

    order by version desc
    limit 1
  {% endset %}

  {% set snapshot_history_result =
    run_query(snapshot_history_sql)
  %}

  {% if snapshot_history_result is none
      or snapshot_history_result.columns | length < 4
      or snapshot_history_result.columns[0].values()
        | length == 0 %}

    {{ return(empty_result) }}

  {% endif %}

  {% set table_version =
    snapshot_history_result
      .columns[0]
      .values()[0]
  %}

  {% set total_rows_inserted_updated =
    snapshot_history_result
      .columns[1]
      .values()[0]
      | int
  %}

  {% set rows_inserted =
    snapshot_history_result
      .columns[2]
      .values()[0]
      | int
  %}

  {% set rows_updated =
    snapshot_history_result
      .columns[3]
      .values()[0]
      | int
  %}

  {% set snapshot_metrics =
    {
      'total_rows_inserted_updated':
        total_rows_inserted_updated,
      'rows_inserted':
        rows_inserted,
      'rows_updated':
        rows_updated
    }
    | tojson
  %}

  {% set snapshot_result = {
    'history_found': true,
    'table_version': table_version | int,
    'total_rows_inserted_updated':
      total_rows_inserted_updated,
    'rows_inserted':
      rows_inserted,
    'rows_updated':
      rows_updated,
    'snapshot_metrics':
      snapshot_metrics
  } %}

  {{ return(snapshot_result) }}

{% endmacro %}

{# ========================================================================= #}
{# RETURN FULLY QUALIFIED TARGET RELATION AS A CLEAN STRING                  #}
{# ========================================================================= #}

{% macro get_target_relation_name(target_relation) %}

  {% if target_relation is none
      or target_relation | string | trim | length == 0 %}

    {{ return('') }}

  {% endif %}

  {#
    relation_name can contain backticks in Databricks:

    `catalog`.`schema`.`table`

    Remove the backticks before storing the value in the
    audit and reconciliation tables.
  #}

  {% set relation_name =
    (target_relation | string)
    | replace('`', '')
    | trim
  %}

  {{ return(relation_name) }}

{% endmacro %}

{% macro is_framework_enabled(model_name) %}

  {% set audit_recon_catalog = var('audit_recon_catalog') %}
  {% set audit_recon_schema = var('audit_recon_schema') %}

  {% set query %}
    select count(*)
    from `{{ audit_recon_catalog }}`.`{{ audit_recon_schema }}`.`audit_batch_control_table`
    where upper(model_name) = upper('{{ model_name }}')
  {% endset %}

  {% set result = run_query(query) %}

  {% if result is not none %}
    {% set cnt = result.columns[0].values()[0] | int %}
    {{ return(cnt > 0) }}
  {% endif %}

  {{ return(false) }}

{% endmacro %}