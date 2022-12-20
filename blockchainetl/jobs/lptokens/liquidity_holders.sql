with token_balance as (
    -- debits
    select to_address as address, contract_address as token, value as value
    from onus.events
    where to_address != '0x0000000000000000000000000000000000000000'
    union all
    -- credits
    select from_address as address, contract_address as token, -value as value
    from onus.events
    where from_address != '0x0000000000000000000000000000000000000000'
    )
select address, token, sum(value) as balance
from token_balance
where token in (SELECT lp_token as token FROM "onus"."lp_tokens")
group by address, token
order by balance desc
limit 10