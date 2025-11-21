{% macro clean_integer(column_name) %}
  COALESCE(
    SAFE_CAST(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          COALESCE(TRIM(CAST({{ adapter.quote(column_name) }} AS STRING)), '0'),
          r'\s', ''
        ),
        r'[^0-9\-]', ''
      ) AS INT64
    ),
    0
  )
{% endmacro %}

