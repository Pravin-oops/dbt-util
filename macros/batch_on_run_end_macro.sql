-- STREAMING_CHUNK: Starting macro definition and initialization
{% macro batch_on_run_end(results) %}

  {% if not execute %}
    {{ return('') }}
  {% endif %}

  {% set job_run_id =
      dbt_shared_utility.get_databricks_job_run_id()
  %}

  {% set validation_failures = [] %}

  {{ log(
      'Starting batch finalisation. '
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
          dbt_shared_utility.is_framework_enabled(model_name)
      %}

      {{ log(
          '                      |   MODEL=' ~ model_name
          ~ '   FRAMEWORK_ENABLED='
          ~ framework_enabled,
          info=True
      ) }}

      {% if not framework_enabled %}

        {{ log(
            'Skipping audit/recon processing for '
            ~ model_name
            ~ ' because model is not configured in batch_control_table',
            info=True
        ) }}

        {% continue %}

      {% endif %}

      {# Existing processing starts here #}

      {% set table_type =
          res.node.resource_type | lower
      %}

      {% set resource_status =
          res.status
          | default('unknown', true)
          | lower
      %}

-- STREAMING_CHUNK: Setting target relation and compiled sql
      {% set target_relation =
          res.node.relation_name
      %}

      {% set compiled_sql =
          res.node.compiled_code
      %}

      {% set transformation_count = 0 %}
      {% set target_count = 0 %}
      {% set error_count = 0 %}
      {% set recon_status = 'SUCCESS' %}
      {% set table_version = none %}
      {% set snapshot_metrics = none %}

      {{ log(
          'Processing RESOURCE='
          ~ model_name
          ~ ' TABLE_TYPE='
          ~ table_type
          ~ ' DBT_STATUS='
          ~ resource_status,
          info=True
      ) }}

      {# ================================================ #}
      {# SUCCESSFUL MODEL OR SNAPSHOT                     #}
      {# ================================================ #}

      {% if resource_status == 'success' %}

        {# ================================================ #}
        {# SOURCE / TRANSFORMATION COUNT                    #}
        {# ================================================ #}

        {% if compiled_sql is none
            or compiled_sql | trim | length == 0 %}

          {% set recon_status =
              'RECON_FAILED'
          %}

          {% do validation_failures.append(
              table_type
              ~ '_'
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

-- STREAMING_CHUNK: Executing transformation query
          {% set transformation_result =
              run_query(
                  transformation_count_sql
              )
          %}

          {% if transformation_result is none
              or transformation_result.columns
                | length == 0
              or transformation_result
                .columns[0]
                .values()
                | length == 0 %}

            {% set recon_status =
                'RECON_FAILED'
            %}

            {% do validation_failures.append(
                table_type
                ~ '_'
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

        {# ================================================ #}
        {# MODEL RECONCILIATION                             #}
        {# EXISTING MODEL LOGIC PRESERVED                   #}
        {# ================================================ #}

        {% if table_type == 'model'
            and recon_status == 'SUCCESS' %}

          {% set target_count_sql %}

            select
              count(*) as target_count
            from {{ target_relation }}
            where audit_load_ts =
            (
              select max(audit_load_ts)
              from {{ target_relation }}
            )

          {% endset %}

-- STREAMING_CHUNK: Executing target count query for models
          {% set target_result =
              run_query(target_count_sql)
          %}

          {% if target_result is none
              or target_result.columns
                | length == 0
              or target_result
                .columns[0]
                .values()
                | length == 0 %}

            {% set recon_status =
                'RECON_FAILED'
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

              {% do validation_failures.append(
                  model_name
                  ~ ': model source count='
                  ~ transformation_count
                  ~ ', target_count='
                  ~ target_count
              ) %}

            {% endif %}

          {% endif %}

          {% set table_version =
              dbt_shared_utility.get_latest_table_version(
                  target_relation
              )
          %}

        {% endif %}

-- STREAMING_CHUNK: Processing snapshot reconciliation
        {# ================================================ #}
        {# SNAPSHOT RECONCILIATION                          #}
        {# ACTIVE SNAPSHOT COUNT RECON                      #}
        {# ================================================ #}
        {% if table_type == 'snapshot'
            and recon_status == 'SUCCESS' %}

          {% set snapshot_details =
              dbt_shared_utility.get_snapshot_audit_details(
                  target_relation
              )
          %}

          {% if not snapshot_details['history_found'] %}

            {% set recon_status =
                'RECON_FAILED'
            %}

            {% do validation_failures.append(
                model_name
                ~ ': snapshot MERGE/CREATE history was not found'
            ) %}

          {% else %}

            {% set table_version =
                snapshot_details['table_version']
            %}

            {% set snapshot_metrics =
                snapshot_details['snapshot_metrics']
            %}

            {% set total_rows_inserted_updated =
                snapshot_details['total_rows_inserted_updated']
                | int
            %}

            {% set rows_inserted =
                snapshot_details['rows_inserted']
                | int
            %}

            {% set rows_updated =
                snapshot_details['rows_updated']
                | int
            %}

            {# ================================================ #}
            {# Active Snapshot Count                            #}
            {# ================================================ #}

            {% set target_count_sql %}
              select count(*) as active_snapshot_count
              from {{ target_relation }}
              where effective_end_date = date '3499-12-31'
            {% endset %}

            {% set target_result =
                run_query(target_count_sql)
            %}

            {% if target_result is none
                or target_result.columns | length == 0
                or target_result.columns[0].values() | length == 0 %}

              {% set recon_status =
                  'RECON_FAILED'
              %}

              {% do validation_failures.append(
                  model_name
                  ~ ': snapshot active count returned no result'
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

                {% do validation_failures.append(
                    model_name
                    ~ ': snapshot source count='
                    ~ transformation_count
                    ~ ', active target count='
                    ~ target_count
                    ~ ', diff='
                    ~ error_count
                    ~ ', rows inserted='
                    ~ rows_inserted
                    ~ ', rows updated='
                    ~ rows_updated
                ) %}

              {% endif %}

            {% endif %}

          {% endif %}

        {% endif %}

-- STREAMING_CHUNK: Writing recon and run records
        {# ================================================ #}
        {# WRITE RECON RECORD                               #}
        {# ================================================ #}

        {% do dbt_shared_utility.batch_recon_upsert(
            model_name=model_name,
            target_relation=target_relation,
            transformation_count=
              transformation_count,
            target_count=target_count,
            error_count=error_count,
            recon_status=recon_status,
            table_type=table_type,
            table_version=table_version,
            snapshot_metrics=snapshot_metrics
        ) %}

        {# ================================================ #}
        {# FINALISE RUN RECORD                              #}
        {# ================================================ #}

        {% do dbt_shared_utility.batch_run_update(
            model_name=model_name,
            final_status=recon_status,
            table_type=table_type,
            table_version=table_version
        ) %}

        {{ log(
            'RESOURCE='
            ~ model_name
            ~ '  TABLE_TYPE='
            ~ table_type
            ~ '  DBT_STATUS='
            ~ resource_status
            ~ '  RECON_STATUS='
            ~ recon_status
            ~ '  SOURCE_COUNT='
            ~ transformation_count
            ~ '  TARGET_COUNT='
            ~ target_count
            ~ '  ERROR_RECORD_COUNT='
            ~ error_count
            ~ '  TABLE_VERSION='
            ~ (
                table_version
                if table_version is not none
                else 'NULL'
              )
            ~ '  SNAPSHOT_METRICS='
            ~ (
                snapshot_metrics
                if snapshot_metrics is not none
                else 'NULL'
              )
            ~ '  DATABRICKS_JOB_RUN_ID='
            ~ job_run_id,
            info=True
        ) }}

-- STREAMING_CHUNK: Handling failed or skipped resources
      {# ================================================ #}
      {# FAILED MODEL OR SNAPSHOT                         #}
      {# ================================================ #}

      {% elif resource_status in
          ['error', 'fail', 'failed'] %}

        {% do dbt_shared_utility.batch_run_update(
            model_name=model_name,
            final_status='FAILED',
            table_type=table_type,
            table_version=none
        ) %}

        {% do dbt_shared_utility.batch_recon_upsert(
            model_name=model_name,
            target_relation=target_relation,
            transformation_count=0,
            target_count=0,
            error_count=0,
            recon_status='FAILED',
            table_type=table_type,
            table_version=none,
            snapshot_metrics=none
        ) %}

        {% set dbt_message =
            res.message
            | default(
                'dbt resource execution failed',
                true
              )
            | string
        %}

        {% do validation_failures.append(
            table_type
            ~ '_'
            ~ model_name
            ~ ': '
            ~ dbt_message
        ) %}

        {{ log(
            'RESOURCE='
            ~ model_name
            ~ '  TABLE_TYPE='
            ~ table_type
            ~ '  STATUS=FAILED'
            ~ '  DATABRICKS_JOB_RUN_ID='
            ~ job_run_id
            ~ '  DBT_MESSAGE='
            ~ dbt_message,
            info=True
        ) }}

      {# ================================================ #}
      {# SKIPPED OR CANCELLED                             #}
      {# ================================================ #}

      {% elif resource_status in
          ['skipped', 'cancelled', 'canceled'] %}

        {% set final_status =
            resource_status | upper
        %}

        {% do dbt_shared_utility.batch_run_update(
            model_name=model_name,
            final_status=final_status,
            table_type=table_type,
            table_version=none
        ) %}

        {% do dbt_shared_utility.batch_recon_upsert(
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
            ~ '_'
            ~ model_name
            ~ ': resource status='
            ~ resource_status
        ) %}

-- STREAMING_CHUNK: Handling unexpected status and completing macro
      {# ================================================ #}
      {# UNEXPECTED STATUS                                #}
      {# ================================================ #}

      {% else %}

        {% set final_status =
            resource_status | upper
        %}

        {% do dbt_shared_utility.batch_run_update(
            model_name=model_name,
            final_status=final_status,
            table_type=table_type,
            table_version=none
        ) %}

        {% do dbt_shared_utility.batch_recon_upsert(
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
            ~ '_'
            ~ model_name
            ~ ': unexpected status='
            ~ resource_status
        ) %}

      {% endif %}

    {% endif %}

  {% endfor %}

  {{ log(
      'Completed batch finalisation. '
      ~ 'DATABRICKS_JOB_RUN_ID='
      ~ job_run_id,
      info=True
  ) }}

  {#
    Raise only after processing every resource.
    This ensures audit rows are completed first.
  #}

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