{% extends "base.sql" %}
{% block main %}
{% set this = ({
        "min_query_date": "2023-09-30",
        "max_query_date": "2023-10-08",
        "freq": "week"
    })
%}

select * 
from {{ alias }}
where 
    date_ts >= '{{min_query_date |default(this.min_query_date) }}'
and date_ts <= '{{max_query_date |default(this.max_query_date) }}'
limit 10;

{% endblock main %}