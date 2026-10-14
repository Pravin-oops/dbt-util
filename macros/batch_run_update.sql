{% macro batch_run_update(
    model_name,
    final_status,
    table_type='model',
    table_version=none
) %}

{% set audit_recon_catalog = var('audit_recon_catalog') %}
{% set audit_recon_schema = var('audit_recon_schema') %}

{% if not execute %}
    {{ return('') }}
{% endif %}

{% set job_run_id =
    dbt_shared_utility.get_databricks_job_run_id()
%}

{% set normalised_table_type =
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

{% set update_sql %}

    update
        `{{ audit_recon_catalog }}`.`{{ audit_recon_schema }}`.`audit_batch_run`

    set
        end_ts =
            current_timestamp(),
        
        status =
            '{{ dbt_shared_utility.sql_escape(final_status) }}',
            
        updated_ts =
            current_timestamp(),
            
        table_type =
            '{{ dbt_shared_utility.sql_escape(normalised_table_type) }}',
            
        table_version =
            {{ table_version_sql }}
            
    where job_run_id =
        '{{ dbt_shared_utility.sql_escape(job_run_id) }}'
        
        and lower(model_name) =
            lower('{{ dbt_shared_utility.sql_escape(model_name) }}')
            
        and lower(
            coalesce(
                table_type,
                'model'
            )
        ) =
        lower(
            '{{ dbt_shared_utility.sql_escape(normalised_table_type) }}'
        )
        
        and batch_id in
        (
            select
                control.batch_id
                
            from
                `{{ audit_recon_catalog }}`.`{{ audit_recon_schema }}`.`audit_batch_control_table`
                as control
                
            where lower(control.model_name) =
                lower(
                    '{{ dbt_shared_utility.sql_escape(model_name) }}'
                )
                
            and lower(
                coalesce(
                    control.table_type,
                    'model'
                )
            ) =
            lower(
                '{{ dbt_shared_utility.sql_escape(normalised_table_type) }}'
            )
            
            and control.active_flag = 'Y'
        )

{% endset %}

{% do run_query(update_sql) %}

{{ return('') }}

{% endmacro %}