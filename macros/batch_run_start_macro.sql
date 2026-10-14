{% macro batch_run_start(
    model_name,
    target_relation,
    table_type='model'
) %}

{% set audit_recon_catalog = var('audit_recon_catalog') %}
{% set audit_recon_schema = var('audit_recon_schema') %}

{% if not execute %}
  {{ return('') }}
{% endif %}

{% set job_run_id =
    dbt_shared_utility.get_databricks_job_run_id()
%}

{% set job_name =
    dbt_shared_utility.get_databricks_job_name()
%}

{% set normalised_table_type =
    table_type | lower
%}

{% set runtime_target_table =
    dbt_shared_utility.get_target_relation_name(target_relation)
%}

merge into
  `{{ audit_recon_catalog }}`.`{{ audit_recon_schema }}`.`audit_batch_run`
  as target

using
  (
    select
      control.batch_id
        as batch_id,

      '{{ dbt_shared_utility.sql_escape(job_run_id) }}'
        as job_run_id,

      '{{ dbt_shared_utility.sql_escape(job_name) }}'
        as job_name,

      control.model_name
        as model_name,

      '{{ dbt_shared_utility.sql_escape(runtime_target_table) }}'
        as target_table_name,

      '{{ dbt_shared_utility.sql_escape(normalised_table_type) }}'
        as table_type,

      cast(null as bigint)
        as table_version,

      current_timestamp()
        as start_ts,

      cast(null as timestamp)
        as end_ts,

      'RUNNING'
        as status,

      current_timestamp()
        as created_ts,

      current_timestamp()
        as updated_ts,

      control.env
        as env,

      control.dataproduct_name
        as dataproduct_name

    from
      `{{ audit_recon_catalog }}`.`{{ audit_recon_schema }}`.`audit_batch_control_table`
      as control

    where lower(control.model_name) =
      lower('{{ dbt_shared_utility.sql_escape(model_name) }}')

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

  ) as source

on target.job_run_id = source.job_run_id

and target.batch_id = source.batch_id

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

  target.job_name =
    source.job_name,

  target.target_table_name =
    source.target_table_name,

  target.table_type =
    source.table_type,

  target.table_version =
    cast(null as bigint),

  target.start_ts =
    source.start_ts,

  target.end_ts =
    cast(null as timestamp),

  target.status =
    'RUNNING',

  target.updated_ts =
    current_timestamp(),

  target.env =
    source.env,

  target.dataproduct_name =
    source.dataproduct_name

when not matched then insert
  (
    batch_id,
    job_run_id,
    job_name,
    model_name,
    target_table_name,
    table_type,
    table_version,
    start_ts,
    end_ts,
    status,
    created_ts,
    updated_ts,
    env,
    dataproduct_name
  )

values
  (
    source.batch_id,
    source.job_run_id,
    source.job_name,
    source.model_name,
    source.target_table_name,
    source.table_type,
    source.table_version,
    source.start_ts,
    source.end_ts,
    source.status,
    source.created_ts,
    source.updated_ts,
    source.env,
    source.dataproduct_name
  )

{% endmacro %}