{% macro batch_run_update(
        model_name,
        final_status,
        table_type='model',
        table_version=none
) %}

    {{ exceptions.raise_compiler_error(
        'batch_run_update is retired. '
        ~ 'audit_batch_run is now append-only. '
        ~ 'Use batch_run_event instead. '
        ~ 'MODEL='
        ~ model_name
        ~ ' STATUS='
        ~ final_status
    ) }}

{% endmacro %}