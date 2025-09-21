spark.sql("""
CREATE OR REPLACE TABLE c1 (
    customer_id STRING,
    address STRING,
    state STRING,
    zip_code_prefix STRING,
    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN
)
PARTITIONED BY (state)
""").show()

sark.sql(
"""
with full_view as(
    select
      COALESCE(dc.customer_id,sc.customer_id) as customer_id,
      sc.address as new_address,
      sc.state as new_state,
      sc.zip_code_prefix as new_zip_code_prefix,
      dc.address as old_address,
      dc.state as old_state,
      dc.zip_code_prefix as old_zip_code_prefix,
      dc.effective_from,
      dc.effective_to,
      dc.is_current
    from c2 sc
    full outer join dim_customer dc on sc.customer_id = dc.customer_id and dc.is_current = true
),expired_records as(
    select
      customer_id,
      old_address as address,
      old_state as state,
      old_zip_code_prefix as zip_code_prefix,
      effective_from,
      current_date() - INTERVAL 1 DAY as effective_to,
      false as is_current
    from full_view
    where old_address is not NULL AND
    (old_address <> new_address OR old_state <> new_state OR old_zip_code_prefix <> new_zip_code_prefix)
),new_records as (
    select
      customer_id,
      new_address as address,
      new_state as state,
      new_zip_code_prefix as zip_code_prefix,
      current_date() as effective_from,
      NULL as effective_to,
      true as is_current
    from full_view
    where old_address is NULL OR old_address <> new_address OR old_state <> new_state OR old_zip_code_prefix <> new_zip_code_prefix
),unchanged_records as (
    select
      customer_id,
      old_address as address,
      old_state as state,
      old_zip_code_prefix as zip_code_prefix,
      effective_from,
      effective_to,
      is_current
    from full_view
    where old_address is not NULL AND
          (new_address=old_address or new_address is NULL)  AND
          (new_state=old_state or new_state is NULL) AND
          (new_zip_code_prefix=old_zip_code_prefix OR new_zip_code_prefix is NULL)
)
SELECT * FROM expired_records
UNION ALL
SELECT * FROM new_records
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM dim_customer WHERE is_current = false
ORDER BY customer_id
""").show(5)


spark.sql(f"""
with full_view as(
    select
        dc.customer_sk,
        COALESCE(dc.customer_id,sc.customer_id) as customer_id,
        sc.address as new_address,
        sc.state as new_state,
        sc.zip_code_prefix as new_zip_code_prefix,
        dc.address as old_address,
        dc.state as old_state,
        dc.zip_code_prefix as old_zip_code_prefix,
        dc.effective_from,
        dc.effective_to,
        dc.is_current
    from {stg_customer_table} sc
    full outer join {dim_customer_table} dc on sc.customer_id = dc.customer_id and dc.is_current = true
),expired_records as(
    select
        customer_sk,
        customer_id,
        old_address as address,
        old_state as state,
        old_zip_code_prefix as zip_code_prefix,
        effective_from,
        current_date() - INTERVAL 1 DAY as effective_to,
        false as is_current
    from full_view
    where old_address is not NULL AND
    (old_address <> new_address OR old_state <> new_state OR old_zip_code_prefix <> new_zip_code_prefix)
),new_records as(
    select
        customer_id,
        new_address as address,
        new_state as state,
        new_zip_code_prefix as zip_code_prefix,
        current_date() as effective_from,
        NULL as effective_to,
        true as is_current
    from full_view
    where old_address is NULL OR old_address <> new_address OR old_state <> new_state OR old_zip_code_prefix <> new_zip_code_prefix
)
select * from expired_records
union all
select sha2(concat_ws('||', customer_id, effective_from), 256) AS customer_sk, * from new_records
""")



-- MERGE INTO {dim_table} AS target
-- USING {stg_dim_table} AS src
-- ON target.customer_sk = src.customer_sk AND target.is_current = true
-- WHEN MATCHED THEN
-- UPDATE SET
--     target.effective_to = src.effective_to,
--     target.is_current = src.is_current
-- WHEN NOT MATCHED THEN
-- INSERT (customer_sk, customer_id, address, state, zip_code_prefix, effective_from, effective_to, is_current)
-- VALUES (src.customer_sk, src.customer_id, src.address, src.state, src.zip_code_prefix, src.effective_from, src.effective_to, src.is_current)
-- WHEN NOT MATCHED BY src THEN
--     UPDATE SET target.is_current = false, target.effective_to = current_date() - INTERVAL 1 DAY
