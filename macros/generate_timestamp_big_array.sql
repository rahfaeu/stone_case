{% macro generate_timestamp_big_array(start_date, end_date) %}

array_concat(
    {% for n in range(10) %}
        {% if not loop.first %},{% endif %} generate_timestamp_array(
            {% if loop.first %}
            cast(cast('{{ start_date }}'as date) as timestamp)
            {% else %}
            date_add(cast(cast('{{ start_date }}'as date) as timestamp), interval {{ n }} minute)
            {% endif %}
            /* go until end of next day */
            , timestamp_trunc(timestamp('{{ end_date }}'), minute)
            , interval 10 minute
        )
    {% endfor %}
)

{% endmacro %}