{% macro batch_run_event(
    model_name,
    target_relation,
    event_type,
    event_status,
    description,
    table_type='model',
    start_table_version=none,
    end_table_version=none,
    event_details=none
) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set audit_recon_catalog =
        var('audit_recon_catalog')
    %}

    {% set audit_recon_schema =
        var('audit_recon_schema')
    %}

    {% set job_run_id =
        get_databricks_job_run_id()
    %}

    {% set job_name =
        get_databricks_job_name()
    %}

    {% set normalised_table_type =
        table_type | lower
    %}

    {% set runtime_target_table =
        get_target_relation_name(
            target_relation
        )
    %}

    {% if start_table_version is none %}
        {% set start_version_sql =
            'cast(null as bigint)'
        %}
    {% else %}
        {% set start_version_sql =
            'cast('
            ~ (start_table_version | string)
            ~ ' as bigint)'
        %}
    {% endif %}

    {% if end_table_version is none %}
        {% set end_version_sql =
            'cast(null as bigint)'
        %}
    {% else %}
        {% set end_version_sql =
            'cast('
            ~ (end_table_version | string)
            ~ ' as bigint)'
        %}
    {% endif %}

    {% if event_details is none %}
        {% set event_details_sql =
            'cast(null as string)'
        %}
    {% else %}
        {% set event_details_sql =
            ''''
            ~ sql_escape(
                event_details
            )
            ~ ''''
        %}
    {% endif %}

    {% set event_sql %}

        insert into
            `default_cib-analytics_4460044220833463`.
            cds_write.
            audit_batch_run
        (
            event_id,
            batch_id,
            job_run_id,
            job_name,
            model_name,
            target_table_name,
            table_type,
            event_type,
            event_status,
            event_ts,
            start_table_version,
            end_table_version,
            description,
            event_details,
            env,
            dataproduct_name,
            created_ts,
            created_by
        )

        select
            uuid()
                as event_id,

            control.batch_id
                as batch_id,

            '{{ sql_escape(
                job_run_id
            ) }}'
                as job_run_id,

            '{{ sql_escape(
                job_name
            ) }}'
                as job_name,

            control.model_name
                as model_name,

            '{{ sql_escape(
                runtime_target_table
            ) }}'
                as target_table_name,

            '{{ sql_escape(
                normalised_table_type
            ) }}'
                as table_type,

            '{{ sql_escape(
                event_type | upper
            ) }}'
                as event_type,

            '{{ sql_escape(
                event_status | upper
            ) }}'
                as event_status,

            current_timestamp()
                as event_ts,

            {{ start_version_sql }}
                as start_table_version,

            {{ end_version_sql }}
                as end_table_version,

            '{{ sql_escape(
                description
            ) }}'
                as description,

            {{ event_details_sql }}
                as event_details,

            control.env
                as env,

            control.dataproduct_name
                as dataproduct_name,

            current_timestamp()
                as created_ts,

            current_user()
                as created_by

        from
            `default_cib-analytics_4460044220833463`.
            cds_write.
            batch_control_table_backup_snapshot_change
                as control

        where lower(control.model_name) =
            lower(
                '{{ sql_escape(
                    model_name
                ) }}'
            )

            and lower(
                coalesce(
                    control.table_type,
                    'model'
                )
            ) =
                lower(
                    '{{ sql_escape(
                        normalised_table_type
                    ) }}'
                )

            and control.active_flag = 'Y'

    {% endset %}

    {% do run_query(event_sql) %}

    {{ log(
        'BATCH_EVENT'
        ~ ' MODEL=' ~ model_name
        ~ ' TABLE_TYPE=' ~ normalised_table_type
        ~ ' EVENT_TYPE=' ~ event_type
        ~ ' EVENT_STATUS=' ~ event_status
        ~ ' START_VERSION='
        ~ (
            start_table_version
            if start_table_version is not none
            else 'NULL'
        )
        ~ ' END_VERSION='
        ~ (
            end_table_version
            if end_table_version is not none
            else 'NULL'
        )
        ~ ' JOB_RUN_ID=' ~ job_run_id,
        info=True
    ) }}

    {{ return('') }}

{% endmacro %}