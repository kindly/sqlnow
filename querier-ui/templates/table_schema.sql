SELECT
    {% for field in schema.fields %}"{{field | first}}"{% if not loop.last %},
    {% endif %}{% endfor %}
FROM
    {{schema.db_name}}
LIMIT 10000