{% macro batch_run_start(
    model_name,
    target_relation,
    table_type='model'
) %}

{% if not execute %}
    {{ return('') }}
{% endif %}

{% set job_run_id =
    get_databricks_job_run_id()
%}

{% set runtime_target_table =
    get_target_relation_name(
        target_relation
    )
%}

{% set start_table_version =
    get_current_table_version(
        target_relation
    )
%}

{% set task_started_event_details =
    {
        "event": "TASK_STARTED",
        "event_source": "pre_hook",
        "hook": "pre_hook",
        "job_run_id": job_run_id,
        "model_name": model_name,
        "resource_type": table_type | lower,
        "target_table": runtime_target_table,
        "execution_status": "STARTED",
        "start_table_version": start_table_version,
        "end_table_version": none,
        "table_changed": false
    }
    | tojson
%}

{% do batch_run_event(
    model_name=model_name,
    target_relation=target_relation,
    event_type='TASK_STARTED',
    event_status='STARTED',
    description=
        'dbt resource execution started by pre-hook',
    table_type=table_type,
    start_table_version=start_table_version,
    end_table_version=none,
    event_details=task_started_event_details
) %}

{{ return('') }}

{% endmacro %}