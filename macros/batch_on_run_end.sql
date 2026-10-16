{% macro batch_on_run_end(results) %}
    {% if not execute %}
        {{ return('') }}
    {% endif %}
    {% set job_run_id =
        get_databricks_job_run_id()
    %}
    {% set validation_failures = [] %}
    {{ log(
        'Starting event-based batch finalisation. '
        ~ 'DATABRICKS_JOB_RUN_ID='
        ~ job_run_id,
        info=True
    ) }}
    {% for res in results %}
        {% if res.node is not none
             and res.node.resource_type in
             ['model', 'snapshot'] %}
            {% set model_name =
                res.node.name
            %}
            {% set framework_enabled =
                is_framework_enabled(
                    model_name
                )
            %}
            {% if not framework_enabled %}
                {{ log(
                    'Skipping audit/recon processing for '
                    ~ model_name
                    ~ ' because it is not enabled in '
                    ~ 'audit_batch_control_table',
                    info=True
                ) }}
                {% continue %}
            {% endif %}
            {% set table_type =
                res.node.resource_type | lower
            %}
            {% set resource_status =
                res.status
                | default('unknown', true)
                | lower
            %}
            {% set target_relation =
                res.node.relation_name
            %}
            {% set compiled_sql =
                res.node.compiled_code
            %}
            {% set dbt_message =
                res.message
                | default('', true)
                | string
            %}
            {% set transformation_count = 0 %}
            {% set target_count = 0 %}
            {% set error_count = 0 %}
            {% set recon_status = 'SUCCESS' %}
            {% set recon_failure_reason = none %}
            {% set end_table_version = none %}
            {% set snapshot_metrics = none %}
            {% set rows_inserted = 0 %}
            {% set rows_updated = 0 %}
            {% set start_table_version =
                get_run_start_table_version(
                    model_name=model_name,
                    table_type=table_type
                )
            %}
            {{ log(
                'Processing RESOURCE='
                ~ model_name
                ~ ' TABLE_TYPE='
                ~ table_type
                ~ ' DBT_STATUS='
                ~ resource_status,
                info=True
            ) }}
            {# ======================================================== #}
            {# SUCCESSFUL MODEL OR SNAPSHOT                             #}
            {# ======================================================== #}
            {% if resource_status == 'success' %}
                {% set end_table_version =
                    get_current_table_version(
                        target_relation
                    )
                %}
                {% set table_changed = false %}
                {% if start_table_version is not none
                     and end_table_version is not none
                     and end_table_version | int
                     > start_table_version | int %}
                    {% set table_changed = true %}
                {% endif %}
                {# ======================================================== #}
                {# TASK COMPLETED EVENT                                     #}
                {# Written before reconciliation so rollback can            #}
                {# detect that dbt successfully changed the table.          #}
                {# ======================================================== #}
                {% set task_completed_event_details =
                    {
                        "event": "TASK_COMPLETED",
                        "event_source": "on_run_end",
                        "hook": "post_processing",
                        "job_run_id": job_run_id,
                        "model_name": model_name,
                        "resource_type": table_type,
                        "target_table": target_relation | string,
                        "dbt_resource_status": resource_status,
                        "execution_status": "SUCCESS",
                        "start_table_version": start_table_version,
                        "end_table_version": end_table_version,
                        "table_changed": table_changed
                    }
                    | tojson
                %}
                {% do batch_run_event(
                    model_name=model_name,
                    target_relation=target_relation,
                    event_type='TASK_COMPLETED',
                    event_status='SUCCESS',
                    description=
                        'dbt resource execution completed successfully',
                    table_type=table_type,
                    start_table_version=start_table_version,
                    end_table_version=end_table_version,
                    event_details=task_completed_event_details
                ) %}
                {# ======================================================== #}
                {# RECON STARTED EVENT                                      #}
                {# ======================================================== #}
                {% set recon_started_event_details =
                    {
                        "event": "RECON_STARTED",
                        "event_source": "on_run_end",
                        "job_run_id": job_run_id,
                        "model_name": model_name,
                        "resource_type": table_type,
                        "target_table": target_relation | string,
                        "dbt_resource_status": resource_status,
                        "reconciliation_status": "STARTED",
                        "start_table_version": start_table_version,
                        "end_table_version": end_table_version,
                        "table_changed": table_changed
                    }
                    | tojson
                %}
                {% do batch_run_event(
                    model_name=model_name,
                    target_relation=target_relation,
                    event_type='RECON_STARTED',
                    event_status='STARTED',
                    description=
                        'Audit and reconciliation processing started',
                    table_type=table_type,
                    start_table_version=start_table_version,
                    end_table_version=end_table_version,
                    event_details=recon_started_event_details
                ) %}
                {# ======================================================== #}
                {# SOURCE / TRANSFORMATION COUNT                            #}
                {# ======================================================== #}
                {% if compiled_sql is none
                     or compiled_sql | trim | length == 0 %}
                    {% set recon_status =
                        'RECON_FAILED'
                    %}
                    {% set recon_failure_reason =
                        'Compiled SQL is unavailable'
                    %}
                    {% do validation_failures.append(
                        table_type
                        ~ '-'
                        ~ model_name
                        ~ ': compiled SQL is unavailable'
                    ) %}
                {% else %}
                    {% set transformation_count_sql %}
                        select
                            count(*) as transformation_count
                        from
                        (
                            {{ compiled_sql }}
                        ) as transformation_output
                    {% endset %}
                    {% set transformation_result =
                        run_query(
                            transformation_count_sql
                        )
                    %}
                    {% if transformation_result is none
                         or transformation_result.columns
                            | length == 0
                         or transformation_result.columns[0]
                            .values()
                            | length == 0 %}
                        {% set recon_status =
                            'RECON_FAILED'
                        %}
                        {% set recon_failure_reason =
                            'Transformation count query returned no result'
                        %}
                        {% do validation_failures.append(
                            table_type
                            ~ '-'
                            ~ model_name
                            ~ ': transformation count returned no result'
                        ) %}
                    {% else %}
                        {% set transformation_count =
                            transformation_result
                            .columns[0]
                            .values()[0]
                            | int
                        %}
                    {% endif %}
                {% endif %}
                {# ======================================================== #}
                {# MODEL RECONCILIATION                                     #}
                {# ======================================================== #}
                {% if table_type == 'model'
                     and recon_status == 'SUCCESS' %}
                    {% set target_count_sql %}
                        select
                            count(*) as target_count
                        from {{ target_relation }}
                        where audit_load_ts =
                        (
                            select
                                max(audit_load_ts)
                            from {{ target_relation }}
                        )
                    {% endset %}
                    {% set target_result =
                        run_query(
                            target_count_sql
                        )
                    %}
                    {% if target_result is none
                         or target_result.columns | length == 0
                         or target_result.columns[0].values()
                            | length == 0 %}
                        {% set recon_status =
                            'RECON_FAILED'
                        %}
                        {% set recon_failure_reason =
                            'Model target count query returned no result'
                        %}
                        {% do validation_failures.append(
                            model_name
                            ~ ': model target count returned no result'
                        ) %}
                    {% else %}
                        {% set target_count =
                            target_result
                            .columns[0]
                            .values()[0]
                            | int
                        %}
                        {% set error_count =
                            (
                                transformation_count
                                - target_count
                            )
                            | abs
                        %}
                        {% if error_count > 0 %}
                            {% set recon_status =
                                'RECON_FAILED'
                            %}
                            {% set recon_failure_reason =
                                'Model source and target counts '
                                ~ 'do not match. Source count='
                                ~ transformation_count
                                ~ ', target count='
                                ~ target_count
                                ~ ', difference='
                                ~ error_count
                            %}
                            {% do validation_failures.append(
                                model_name
                                ~ ': model source count='
                                ~ transformation_count
                                ~ ', target count='
                                ~ target_count
                                ~ ', difference='
                                ~ error_count
                            ) %}
                        {% endif %}
                    {% endif %}
                {% endif %}
                {# ======================================================== #}
                {# SNAPSHOT RECONCILIATION                                  #}
                {# ======================================================== #}
                {% if table_type == 'snapshot'
                     and recon_status == 'SUCCESS' %}
                    {% set snapshot_details =
                        get_snapshot_audit_details(
                            target_relation
                        )
                    %}
                    {% if not snapshot_details['history_found'] %}
                        {% set recon_status =
                            'RECON_FAILED'
                        %}
                        {% set recon_failure_reason =
                            'Snapshot MERGE or CREATE history '
                            ~ 'was not found'
                        %}
                        {% do validation_failures.append(
                            model_name
                            ~ ': snapshot MERGE/CREATE history '
                            ~ 'was not found'
                        ) %}
                    {% else %}
                        {% set end_table_version =
                            snapshot_details['table_version']
                        %}
                        {% set snapshot_metrics =
                            snapshot_details['snapshot_metrics']
                        %}
                        {% set rows_inserted =
                            snapshot_details['rows_inserted']
                            | int
                        %}
                        {% set rows_updated =
                            snapshot_details['rows_updated']
                            | int
                        %}
                        {% set table_changed = false %}
                        {% if start_table_version is not none
                             and end_table_version is not none
                             and end_table_version | int
                             > start_table_version | int %}
                            {% set table_changed = true %}
                        {% endif %}
                        {% set target_count_sql %}
                            select
                                count(*) as active_snapshot_count
                            from {{ target_relation }}
                            where effective_end_date =
                                date '3499-12-31'
                        {% endset %}
                        {% set target_result =
                            run_query(
                                target_count_sql
                            )
                        %}
                        {% if target_result is none
                             or target_result.columns | length == 0
                             or target_result.columns[0].values()
                                | length == 0 %}
                            {% set recon_status =
                                'RECON_FAILED'
                            %}
                            {% set recon_failure_reason =
                                'Snapshot active record count '
                                ~ 'query returned no result'
                            %}
                            {% do validation_failures.append(
                                model_name
                                ~ ': snapshot active count '
                                ~ 'returned no result'
                            ) %}
                        {% else %}
                            {% set target_count =
                                target_result
                                .columns[0]
                                .values()[0]
                                | int
                            %}
                            {% set error_count =
                                (
                                    transformation_count
                                    - target_count
                                )
                                | abs
                            %}
                            {% if error_count > 0 %}
                                {% set recon_status =
                                    'RECON_FAILED'
                                %}
                                {% set recon_failure_reason =
                                    'Snapshot source and active target '
                                    ~ 'counts do not match. Source count='
                                    ~ transformation_count
                                    ~ ', active target count='
                                    ~ target_count
                                    ~ ', difference='
                                    ~ error_count
                                    ~ ', rows_inserted='
                                    ~ rows_inserted
                                    ~ ', rows_updated='
                                    ~ rows_updated
                                %}
                                {% do validation_failures.append(
                                    model_name
                                    ~ ': snapshot source count='
                                    ~ transformation_count
                                    ~ ', active target count='
                                    ~ target_count
                                    ~ ', diff='
                                    ~ error_count
                                    ~ ', rows_inserted='
                                    ~ rows_inserted
                                    ~ ', rows_updated='
                                    ~ rows_updated
                                ) %}
                            {% endif %}
                        {% endif %}
                    {% endif %}
                {% endif %}
                {# ======================================================== #}
                {# UPDATE RECON TABLE                                       #}
                {# ======================================================== #}
                {% do batch_recon_upsert(
                    model_name=model_name,
                    target_relation=target_relation,
                    transformation_count=transformation_count,
                    target_count=target_count,
                    error_count=error_count,
                    recon_status=recon_status,
                    table_type=table_type,
                    table_version=end_table_version,
                    snapshot_metrics=snapshot_metrics
                ) %}
                {# ======================================================== #}
                {# FINAL RECON EVENT DETAILS                                #}
                {# ======================================================== #}
                {% if recon_status == 'SUCCESS' %}
                    {% set final_recon_event =
                        'RECON_SUCCESS'
                    %}
                {% else %}
                    {% set final_recon_event =
                        'RECON_FAILED'
                    %}
                {% endif %}
                {% set recon_event_details =
                    {
                        "event": final_recon_event,
                        "event_source": "on_run_end",
                        "job_run_id": job_run_id,
                        "model_name": model_name,
                        "resource_type": table_type,
                        "target_table": target_relation | string,
                        "dbt_resource_status": resource_status,
                        "reconciliation_status": recon_status,
                        "source_count": transformation_count,
                        "target_count": target_count,
                        "error_count": error_count,
                        "count_difference": error_count,
                        "start_table_version": start_table_version,
                        "end_table_version": end_table_version,
                        "table_changed": table_changed,
                        "rows_inserted": (
                            rows_inserted
                            if table_type == 'snapshot'
                            else none
                        ),
                        "rows_updated": (
                            rows_updated
                            if table_type == 'snapshot'
                            else none
                        ),
                        "snapshot_metrics": (
                            snapshot_metrics
                            if table_type == 'snapshot'
                            else none
                        ),
                        "failure_reason": recon_failure_reason
                    }
                    | tojson
                %}
                {% if recon_status == 'SUCCESS' %}
                    {% do batch_run_event(
                        model_name=model_name,
                        target_relation=target_relation,
                        event_type='RECON_SUCCESS',
                        event_status='SUCCESS',
                        description=
                            'Audit and reconciliation completed '
                            ~ 'successfully. Source count='
                            ~ transformation_count
                            ~ ', target count='
                            ~ target_count
                            ~ ', error count='
                            ~ error_count,
                        table_type=table_type,
                        start_table_version=start_table_version,
                        end_table_version=end_table_version,
                        event_details=recon_event_details
                    ) %}
                {% else %}
                    {% if recon_failure_reason is none %}
                        {% set recon_failure_reason =
                            'Audit and reconciliation validation failed'
                        %}
                    {% endif %}
                    {% do batch_run_event(
                        model_name=model_name,
                        target_relation=target_relation,
                        event_type='RECON_FAILED',
                        event_status='FAILED',
                        description=recon_failure_reason,
                        table_type=table_type,
                        start_table_version=start_table_version,
                        end_table_version=end_table_version,
                        event_details=recon_event_details
                    ) %}
                {% endif %}
            {# ======================================================== #}
            {# FAILED MODEL OR SNAPSHOT                                 #}
            {# ======================================================== #}
            {% elif resource_status in
                ['error', 'fail', 'failed'] %}
                {% if target_relation is not none %}
                    {% set failed_table_version =
                        get_current_table_version(
                            target_relation
                        )
                    %}
                {% else %}
                    {% set failed_table_version = none %}
                {% endif %}
                {% set dbt_message =
                    res.message
                    | default(
                        'dbt resource execution failed',
                        true
                    )
                    | string
                %}
                {% set table_may_have_changed = false %}
                {% if start_table_version is not none
                     and failed_table_version is not none
                     and failed_table_version | int
                     > start_table_version | int %}
                    {% set table_may_have_changed = true %}
                {% endif %}
                {% set task_failed_event_details =
                    {
                        "event": "TASK_FAILED",
                        "event_source": "on_run_end",
                        "job_run_id": job_run_id,
                        "model_name": model_name,
                        "resource_type": table_type,
                        "target_table": (
                            target_relation | string
                            if target_relation is not none
                            else none
                        ),
                        "dbt_resource_status": resource_status,
                        "execution_status": "FAILED",
                        "failure_type": "DBT_RESOURCE_FAILURE",
                        "start_table_version": start_table_version,
                        "current_table_version": failed_table_version,
                        "end_table_version": failed_table_version,
                        "table_may_have_changed": table_may_have_changed,
                        "error_message": dbt_message
                    }
                    | tojson
                %}
                {% do batch_run_event(
                    model_name=model_name,
                    target_relation=target_relation,
                    event_type='TASK_FAILED',
                    event_status='FAILED',
                    description=dbt_message,
                    table_type=table_type,
                    start_table_version=start_table_version,
                    end_table_version=failed_table_version,
                    event_details=task_failed_event_details
                ) %}
                {% do batch_recon_upsert(
                    model_name=model_name,
                    target_relation=target_relation,
                    transformation_count=0,
                    target_count=0,
                    error_count=0,
                    recon_status='FAILED',
                    table_type=table_type,
                    table_version=failed_table_version,
                    snapshot_metrics=none
                ) %}
                {% do validation_failures.append(
                    table_type
                    ~ '-'
                    ~ model_name
                    ~ ':'
                    ~ dbt_message
                ) %}
            {# ======================================================== #}
            {# SKIPPED OR CANCELLED                                     #}
            {# ======================================================== #}
            {% elif resource_status in
                ['skipped', 'cancelled', 'canceled'] %}
                {% set final_status =
                    resource_status | upper
                %}
                {% if resource_status in
                     ['cancelled', 'canceled'] %}
                    {% set event_type =
                        'TASK_CANCELLED'
                    %}
                    {% set incomplete_reason =
                        "Resource execution was cancelled"
                    %}
                {% else %}
                    {% set event_type =
                        'TASK_SKIPPED'
                    %}
                    {% set incomplete_reason =
                        "Resource execution was skipped"
                    %}
                {% endif %}
                {% set incomplete_task_event_details =
                    {
                        "event": event_type,
                        "event_source": "on_run_end",
                        "job_run_id": job_run_id,
                        "model_name": model_name,
                        "resource_type": table_type,
                        "target_table": (
                            target_relation | string
                            if target_relation is not none
                            else none
                        ),
                        "dbt_resource_status": resource_status,
                        "execution_status": final_status,
                        "start_table_version": start_table_version,
                        "end_table_version": none,
                        "table_changed": false,
                        "reason": incomplete_reason
                    }
                    | tojson
                %}
                {% do batch_run_event(
                    model_name=model_name,
                    target_relation=target_relation,
                    event_type=event_type,
                    event_status=final_status,
                    description=
                        'dbt resource did not complete; status='
                        ~ resource_status,
                    table_type=table_type,
                    start_table_version=start_table_version,
                    end_table_version=none,
                    event_details=incomplete_task_event_details
                ) %}
                {% do batch_recon_upsert(
                    model_name=model_name,
                    target_relation=target_relation,
                    transformation_count=0,
                    target_count=0,
                    error_count=0,
                    recon_status=final_status,
                    table_type=table_type,
                    table_version=none,
                    snapshot_metrics=none
                ) %}
                {% do validation_failures.append(
                    table_type
                    ~ '-'
                    ~ model_name
                    ~ ': resource status='
                    ~ resource_status
                ) %}
            {# ======================================================== #}
            {# UNEXPECTED STATUS                                        #}
            {# ======================================================== #}
            {% else %}
                {% set final_status =
                    resource_status | upper
                %}
                {% set unexpected_error_message =
                    'Unexpected dbt resource status='
                    ~ resource_status
                %}
                {% set unexpected_status_event_details =
                    {
                        "event": "TASK_FAILED",
                        "event_source": "on_run_end",
                        "job_run_id": job_run_id,
                        "model_name": model_name,
                        "resource_type": table_type,
                        "target_table": (
                            target_relation | string
                            if target_relation is not none
                            else none
                        ),
                        "dbt_resource_status": resource_status,
                        "execution_status": final_status,
                        "failure_type": "UNEXPECTED_DBT_STATUS",
                        "start_table_version": start_table_version,
                        "end_table_version": none,
                        "table_changed": false,
                        "error_message": unexpected_error_message
                    }
                    | tojson
                %}
                {% do batch_run_event(
                    model_name=model_name,
                    target_relation=target_relation,
                    event_type='TASK_FAILED',
                    event_status=final_status,
                    description=unexpected_error_message,
                    table_type=table_type,
                    start_table_version=start_table_version,
                    end_table_version=none,
                    event_details=unexpected_status_event_details
                ) %}
                {% do batch_recon_upsert(
                    model_name=model_name,
                    target_relation=target_relation,
                    transformation_count=0,
                    target_count=0,
                    error_count=0,
                    recon_status=final_status,
                    table_type=table_type,
                    table_version=none,
                    snapshot_metrics=none
                ) %}
                {% do validation_failures.append(
                    table_type
                    ~ '-'
                    ~ model_name
                    ~ ': unexpected status='
                    ~ resource_status
                ) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {{ log(
        'Completed event-based batch finalisation. '
        ~ 'DATABRICKS_JOB_RUN_ID='
        ~ job_run_id,
        info=True
    ) }}
    {% if validation_failures | length > 0 %}
        {% set combined_failure_message =
            validation_failures | join(' | ')
        %}
        {{ exceptions.raise_compiler_error(
            'Batch/reconciliation validation failed: '
            ~ combined_failure_message
        ) }}
    {% endif %}
    {{ return('') }}
{% endmacro %}
