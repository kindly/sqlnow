SELECT
    {% for field in schema.fields %}"{{field | first}}"{% if not loop.last %},{% else %} {% endif %}{{field|first|pad(max_field_length)}}-- {{field | last}}
    {% endfor %}
FROM
    {{schema.db_name}}
LIMIT 10000