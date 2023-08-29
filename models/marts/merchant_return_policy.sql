with merchant_return_policy_current_state as(
  select r.shop_id
        ,json_value(parse_json(r.return_policy),'$.gift_cards_enabled') current_status_gift_cards_enabled_ind
        ,json_value(parse_json(r.return_policy),'$.instant_exchange_enabled') current_status_instant_exchange_enabled_ind
        ,json_value(parse_json(r.return_policy),'$.refunds_enabled') current_status_refunds_enabled_ind
        ,json_value(parse_json(r.return_policy),'$.persistent_credit_enabled') current_status_persistent_credit_enabled_ind
        ,json_value(parse_json(r.return_policy),'$.keep_item_threshold') current_status_keep_item_threshold
        ,r.return_policy
  from `dbt_cnolan.returns` r
  qualify row_number() over (partition by r.shop_id order by r.return_created_at desc) = 1
)

select 'O-'||o.order_id model_key
      ,'ORDER' return_or_order
      ,o.order_id return_order_id
      ,o.order_id
      ,o.order_created_at created_at
      ,o.line_item_count item_count 
      ,o.gross_merchandise_value_usd
      ,mrpcs.current_status_gift_cards_enabled_ind
      ,mrpcs.current_status_instant_exchange_enabled_ind
      ,mrpcs.current_status_refunds_enabled_ind
      ,mrpcs.current_status_persistent_credit_enabled_ind
      ,mrpcs.current_status_keep_item_threshold
      ,mrpcs.return_policy
from `dbt_cnolan.orders` o
left join merchant_return_policy_current_state mrpcs on o.shop_id = mrpcs.shop_id

union all

select 'R-'||r.return_id model_key
      ,'RETURN' return_or_order
      ,r.return_id return_order_id
      ,r.order_id
      ,cast(substring(r.return_created_at,1,19) as datetime) created_at
      ,r.item_count item_count 
      ,null gross_merchandise_value_usd
      ,mrpcs.current_status_gift_cards_enabled_ind
      ,mrpcs.current_status_instant_exchange_enabled_ind
      ,mrpcs.current_status_refunds_enabled_ind
      ,mrpcs.current_status_persistent_credit_enabled_ind
      ,mrpcs.current_status_keep_item_threshold
      ,mrpcs.return_policy
from `dbt_cnolan.returns` r
left join merchant_return_policy_current_state mrpcs on r.shop_id = mrpcs.shop_id