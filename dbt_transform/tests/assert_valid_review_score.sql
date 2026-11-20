select *
from {{ ref('fact_orders') }}
where review_score is not null
  and (review_score < 1.0 or review_score > 5.0)
