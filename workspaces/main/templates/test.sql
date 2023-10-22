{% extends "base.sql" %}
{% block main %}
{% set this = ({
        "min_query_date": "2022-09-30",
        "max_query_date": "2023-09-30",
    })
%}

with stage as (
select dates, orders, gmv, region, source,
{{ macros.ma('gmv',4,order='dates',partition='region,source') }} as gmv_ma4
from '{{table}}'
where dates>'{{min_query_date |default(this.min_query_date) }}'
and dates<'{{max_query_date |default(this.max_query_date) }}'
)
select * from stage;
{% endblock main %}