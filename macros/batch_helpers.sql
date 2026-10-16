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


{# ================================================================= #}
{# RETURN LATEST TABLE VERSION                                       #}
{# Used for successful models                                        #}
{# ================================================================= #}

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


{# ================================================================= #}
{# RETURN SNAPSHOT METRICS AND SNAPSHOT TABLE VERSION                #}
{# ================================================================= #}

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
        SELECT the latest snapshot write.
        MERGE is used for normal dbt snapshot processing.
        CREATE operations are included for the first execution,
        where the snapshot target table may be created.
    #}

    {% set snapshot_history_sql %}

        select

            cast(version as bigint)
                as table_version,

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

                when upper(operation) = 'MERGE'
                then
                    cast(
                        coalesce(
                            operationMetrics['numTargetRowsInserted'],
                            '0'
                        ) as bigint
                    )
                    +
                    cast(
                        coalesce(
                            operationMetrics['numTargetRowsMatchedUpdated'],
                            operationMetrics['numTargetRowsUpdated'],
                            '0'
                        ) as bigint
                    )

                else 0

            end as total_rows_inserted_updated,

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



{% macro is_framework_enabled(model_name) %}

    {% set query %}
        select count(*)
        from `default_cib-analytics_4460044220833463`.`cds_write`.`batch_control_table_backup_snapshot_change`
        where upper(model_name) = upper('{{ model_name }}')
    {% endset %}

    {% set result = run_query(query) %}

    {% if result is not none %}
        {% set cnt = result.columns[0].values()[0] | int %}
        {{ return(cnt > 0) }}
    {% endif %}

    {{ return(false) }}

{% endmacro %}


{# ================================================================= #}
{# RETURN CURRENT TABLE VERSION SAFELY                               #}
{# ================================================================= #}

{% macro get_current_table_version(target_relation) %}

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


{# ================================================================= #}
{# RETURN START VERSION CAPTURED BY PRE-HOOK                         #}
{# ================================================================= #}

{% macro get_run_start_table_version(
    model_name,
    table_type='model'
) %}

    {% if not execute %}
        {{ return(none) }}
    {% endif %}

    {% set audit_recon_catalog = var('audit_recon_catalog') %}
    {% set audit_recon_schema = var('audit_recon_schema') %}

    {% set job_run_id =
        get_databricks_job_run_id()
    %}

    {% set normalised_table_type =
        table_type | lower
    %}

    {% set start_version_sql %}

        select
            start_table_version
        from
            `default_cib-analytics_4460044220833463`.
            `cds_write`.
            `audit_batch_run`
        where job_run_id =
            '{{ sql_escape(job_run_id) }}'

            and lower(model_name) =
                lower(
                    '{{ sql_escape(model_name) }}'
                )

            and lower(
                coalesce(
                    table_type,
                    'model'
                )
            ) =
                lower(
                    '{{ sql_escape(
                        normalised_table_type
                    ) }}'
                )

            and event_type = 'TASK_STARTED'

        order by event_ts desc
        limit 1

    {% endset %}

    {% set start_version_result =
        run_query(start_version_sql)
    %}

    {% if start_version_result is none
        or start_version_result.columns | length == 0
        or start_version_result.columns[0].values()
        | length == 0 %}
        {{ return(none) }}
    {% endif %}

    {% set start_version =
        start_version_result.columns[0].values()[0]
    %}

    {% if start_version is none %}
        {{ return(none) }}
    {% endif %}

    {{ return(start_version | int) }}

{% endmacro %}

{% macro get_target_relation_name(target_relation) %}

    {% if target_relation is none %}
        {{ return('') }}
    {% endif %}

    {% if target_relation.identifier is defined %}
        {{ return(target_relation.identifier) }}
    {% endif %}

    {{ return(target_relation|string) }}

{% endmacro %}