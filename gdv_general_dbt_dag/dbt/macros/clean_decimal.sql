{% macro clean_decimal(column_name) %}
  COALESCE(
    ROUND(
      SAFE_CAST(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              COALESCE(TRIM(CAST({{ adapter.quote(column_name) }} AS STRING)), '0'),
              r'\s', ''
            ),
            r'[^0-9.,\-]', ''
          ),
          r',', '.'
        ) AS FLOAT64
      ),
      2
    ),
    0
  )
{% endmacro %}

