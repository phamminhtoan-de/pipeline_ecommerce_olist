select *
from {{ ref('fact_orders') }}
where purchase_date_key > 0
  and (
      (approved_date_key > 0 and approved_date_key < purchase_date_key)
      or (delivered_date_key > 0 and delivered_date_key < purchase_date_key)
  )
