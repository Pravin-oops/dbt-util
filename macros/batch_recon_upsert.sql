{% macro batch_recon_upsert(    
    model_name,
    target_relation,
    transformation_count,
    target_count,
    error_count,
    recon_status,
    table_type='model',
    table_version=none,
    snapshot_metrics=none
) %}

    {% if not execute %}
        {{ return('') }}
    {% endif %}

    {% set job_run_id =
        get_databricks_job_run_id()
    %}

    {% set normalized_table_type =
        table_type | lower
    %}

    {% if table_version is none %}
        {% set table_version_sql =
            'cast(null as bigint)'
        %}
    {% else %}
        {% set table_version_sql =
            'cast('
            ~ (table_version | string)
            ~ ' as bigint)'
        %}
    {% endif %}

    {% if snapshot_metrics is none %}
        {% set snapshot_metrics_sql =
            'cast(null as string)'
        %}
    {% else %}
        {% set snapshot_metrics_sql =
            "'"
            ~ sql_escape(snapshot_metrics)
            ~ "'"
        %}
    {% endif %}

    {% set recon_sql %}

        merge into
            `default_cib-analytics_4460044220833463`.`cds_write`.`batch_recon_backup_snapshot_change`
            as target

        using
        (
            select
                concat(
                    cast(control.batch_id as string),
                    '_',
                    '{{ sql_escape(job_run_id) }}',
                    '_',
                    '{{ sql_escape(normalized_table_type) }}',
                    '_',
                    control.model_name
                ) as unique_id,

                control.batch_id
                    as batch_id,

                '{{ sql_escape(job_run_id) }}'
                    as job_run_id,

                control.model_name
                    as model_name,

                control.env
                    as env,

                control.dataproduct_name
                    as dataproduct_name,

                control.target_table_name
                    as target_table_name,

                '{{ sql_escape(normalized_table_type) }}'
                    as table_type,

                {{ table_version_sql }}
                    as table_version,

                {{ snapshot_metrics_sql }}
                    as snapshot_metrics,

                cast(
                    {{ transformation_count }}
                    as bigint
                ) as source_count,

                cast(
                    {{ target_count }}
                    as bigint
                ) as target_count,

                cast(
                    {{ error_count }}
                    as bigint
                ) as error_record_count,

                '{{ sql_escape(recon_status) }}'
                    as status,

                current_timestamp()
                    as last_modified_ts,

                current_user()
                    as last_modified_by

            from
                `default_cib-analytics_4460044220833463`.`cds_write`.`batch_control_table_backup_snapshot_change`
                as control

            where lower(control.model_name) =
                lower(
                    '{{ sql_escape(model_name) }}'
                )

                and lower(
                    coalesce(
                        control.table_type,
                        'model'
                    )
                ) =
                lower(
                    '{{ sql_escape(normalized_table_type) }}'
                )

                and control.active_flag = 'Y'

        ) as source

        on  target.job_run_id =
            source.job_run_id

        and target.batch_id =
            source.batch_id

        and lower(target.model_name) =
            lower(source.model_name)

        and lower(
            coalesce(
                target.table_type,
                'model'
            )
        ) =
        lower(source.table_type)

        when matched then update set

            target.unique_id =
                source.unique_id,

            target.env =
                source.env,

            target.dataproduct_name =
                source.dataproduct_name,

            target.target_table_name =
                source.target_table_name,

            target.table_type =
                source.table_type,

            target.table_version =
                source.table_version,

            target.snapshot_metrics =
                source.snapshot_metrics,

            target.source_count =
                source.source_count,

            target.target_count =
                source.target_count,

            target.error_record_count =
                source.error_record_count,

            target.status =
                source.status,

            target.last_modified_ts =
                source.last_modified_ts,

            target.last_modified_by =
                source.last_modified_by

        when not matched then insert
        (
            unique_id,
            batch_id,
            job_run_id,
            model_name,
            env,
            dataproduct_name,
            target_table_name,
            table_type,
            table_version,
            snapshot_metrics,
            source_count,
            target_count,
            error_record_count,
            status,
            last_modified_ts,
            last_modified_by
        )

        values
        (
            source.unique_id,
            source.batch_id,
            source.job_run_id,
            source.model_name,
            source.env,
            source.dataproduct_name,
            source.target_table_name,
            source.table_type,
            source.table_version,
            source.snapshot_metrics,
            source.source_count,
            source.target_count,
            source.error_record_count,
            source.status,
            source.last_modified_ts,
            source.last_modified_by
        )

    {% endset %}

    {% do run_query(recon_sql) %}

    {{ return('') }}

{% endmacro %}