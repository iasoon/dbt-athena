{% materialization table, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {{ run_hooks(pre_hooks) }}

  {%- if old_relation is none -%}
    {% call statement('main') -%}
      {{ create_table_as(False, target_relation, sql) }}
    {% endcall -%}
  {%- else -%}
    {%- set temp_relation = adapter.create_temp_table(database=database, schema=schema) -%}
    {% call statement('main') -%}
      {{ create_table_as(False, temp_relation, sql) }}
    {% endcall -%}
    {% do adapter.replace_table(target_relation, temp_relation) %}
  {%- endif -%}


  {{ set_table_classification(target_relation, 'parquet') }}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
