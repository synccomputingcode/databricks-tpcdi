-- Databricks notebook source
-- MAGIC %py
-- MAGIC dbutils.widgets.text("catalog_name", "stewart", "Catalog Name")
-- MAGIC dbutils.widgets.text("scaling_factor", "10", "Scaling Factor")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #dbutils.widgets.removeAll()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC A simple notebook for building the Medallion Lakehouse architecture from the TPC-DI source files.
-- MAGIC This builds exactly what the dbt models in this repository build.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Set the desired catalog, and create the Medallion schemas.

-- COMMAND ----------

USE CATALOG ${catalog_name};
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- COMMAND ----------

-- DROP SCHEMA IF EXISTS bronze CASCADE;
-- DROP SCHEMA IF EXISTS silver CASCADE;
-- DROP SCHEMA IF EXISTS gold CASCADE;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create bronze tables for all the data ingested from the Brokerage transactional system.

-- COMMAND ----------

create
or replace table bronze.brokerage_cash_transaction as (
  select
    *
  from
    tpcdi_sf${scaling_factor}.cash_transaction
);

-- COMMAND ----------

create
or replace table bronze.brokerage_daily_market as (
  select
    *
  from
    tpcdi_sf${scaling_factor}.daily_market
);

-- COMMAND ----------

create
or replace table bronze.brokerage_holding_history as (
  select
    *
  from
    tpcdi_sf${scaling_factor}.holding_history
);

-- COMMAND ----------

create
or replace table bronze.brokerage_trade_history as
select
  *
from
  tpcdi_sf${scaling_factor}.trade_history;

-- COMMAND ----------

create
or replace table bronze.brokerage_trade as
select
  *
from
  tpcdi_sf${scaling_factor}.trade;

-- COMMAND ----------

create
or replace table bronze.brokerage_watch_history as
select
  *
from
  tpcdi_sf${scaling_factor}.watch_history;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create bronze tables for all the data ingested from the CRM system.

-- COMMAND ----------

create
or replace table bronze.crm_customer_mgmt as
select
  *
from
  tpcdi_sf${scaling_factor}.customer_mgmt;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create bronze tables for all the data ingested from the Financial Wire files.

-- COMMAND ----------

create
or replace table bronze.finwire_company as (
  select
    *
  from
    tpcdi_sf${scaling_factor}.cmp
);

-- COMMAND ----------

create
or replace table bronze.finwire_financial as (
  with s1 as (
    select
      *,
      try_cast(co_name_or_cik as integer) as try_cik
    from
      tpcdi_sf${scaling_factor}.fin
  )
  select
    pts,
    cast(year as integer) as year,
    cast(quarter as integer) as quarter,
    to_date(quarter_start_date, 'yyyymmdd') as quarter_start_date,
    to_date(posting_date, 'yyyymmdd') as posting_date,
    cast(revenue as float) as revenue,
    cast(earnings as float) as earnings,
    cast(eps as float) as eps,
    cast(diluted_eps as float) as diluted_eps,
    cast(margin as float) as margin,
    cast(inventory as float) as inventory,
    cast(assets as float) as assets,
    cast(liabilities as float) as liabilities,
    cast(sh_out as integer) as sh_out,
    cast(diluted_sh_out as integer) as diluted_sh_out,
    try_cik cik,
    case
      when try_cik is null then co_name_or_cik
      else null
    end company_name
  from
    s1
);

-- COMMAND ----------

create
or replace table bronze.finwire_security as (
  with s1 as (
    select
      *,
      cast(co_name_or_cik as integer) as try_cik
    from
      tpcdi_sf${scaling_factor}.sec
  )
  select
    pts,
    symbol,
    issue_type,
    status,
    name,
    ex_id,
    cast(sh_out as integer) as sh_out,
    to_date(first_trade_date, 'yyyymmdd') as first_trade_date,
    to_date(first_exchange_date, 'yyyymmdd') as first_exchange_date,
    cast(dividend as float) as dividend,
    try_cik cik,
    case
      when try_cik is null then co_name_or_cik
      else null
    end company_name
  from
    s1
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create bronze tables for all the data ingested from the HR system.

-- COMMAND ----------

create
or replace table bronze.hr_employee as (
  select
    *
  from
    tpcdi_sf${scaling_factor}.hr
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create bronze tables for all the reference data.

-- COMMAND ----------

create
or replace table bronze.reference_date as (
  select
    DATE_VALUE SK_DATE_ID,
    DATE_VALUE,
    DATE_DESC,
    CALENDAR_YEAR_ID,
    CALENDAR_YEAR_DESC,
    CALENDAR_QTR_ID,
    CALENDAR_QTR_DESC,
    CALENDAR_MONTH_ID,
    CALENDAR_MONTH_DESC,
    CALENDAR_WEEK_ID,
    CALENDAR_WEEK_DESC,
    DAY_OF_WEEK_NUM,
    DAY_OF_WEEK_DESC,
    FISCAL_YEAR_ID,
    FISCAL_YEAR_DESC,
    FISCAL_QTR_ID,
    FISCAL_QTR_DESC,
    HOLIDAY_FLAG
  from
    tpcdi_sf${scaling_factor}.date
);

-- COMMAND ----------

create
or replace table bronze.reference_industry as (
  select
    *
  from
    tpcdi_sf${scaling_factor}.industry
);

-- COMMAND ----------

create
or replace table bronze.reference_status_type as (
  select
    *
  from
    tpcdi_sf${scaling_factor}.status_type
);

-- COMMAND ----------

create
or replace table bronze.reference_tax_rate as (
  select
    *
  from
    tpcdi_sf${scaling_factor}.tax_rate
);

-- COMMAND ----------

create
or replace table bronze.reference_trade_type as (
  select
    *
  from
    tpcdi_sf${scaling_factor}.trade_type
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create bronze tables for syndicated data.

-- COMMAND ----------

create
or replace table bronze.syndicated_prospect as (
  select
    *
  from
    tpcdi_sf${scaling_factor}.prospect
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Load the business entities in the Silver zone.

-- COMMAND ----------

create
or replace table silver.accounts as (
  select
    action_type,
    decode(
      action_type,
      'NEW',
      'Active',
      'ADDACCT',
      'Active',
      'UPDACCT',
      'Active',
      'CLOSEACCT',
      'Inactive'
    ) status,
    ca_id account_id,
    ca_name account_desc,
    c_id customer_id,
    c_tax_id tax_id,
    c_gndr gender,
    c_tier tier,
    c_dob dob,
    c_l_name last_name,
    c_f_name first_name,
    c_m_name middle_name,
    c_adline1 address_line1,
    c_adline2 address_line2,
    c_zipcode postal_code,
    c_city city,
    c_state_prov state_province,
    c_ctry country,
    c_prim_email primary_email,
    c_alt_email alternate_email,
    c_phone_1 phone1,
    c_phone_2 phone2,
    c_phone_3 phone3,
    c_lcl_tx_id local_tax_rate_name,
    ltx.tx_rate local_tax_rate,
    c_nat_tx_id national_tax_rate_name,
    ntx.tx_rate national_tax_rate,
    ca_tax_st tax_status,
    ca_b_id broker_id,
    action_ts as effective_timestamp,
    ifnull(
      timestampadd(
        MILLISECOND,
        -1,
        lag(action_ts) over (
          partition by ca_id
          order by
            action_ts desc
        )
      ),
      to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    CASE
      WHEN (
        row_number() over (
          partition by ca_id
          order by
            action_ts desc
        ) = 1
      ) THEN TRUE
      ELSE FALSE
    END as IS_CURRENT
  from
    bronze.crm_customer_mgmt c
    left join bronze.reference_tax_rate ntx on c.c_nat_tx_id = ntx.tx_id
    left join bronze.reference_tax_rate ltx on c.c_lcl_tx_id = ltx.tx_id
  where
    ca_id is not null
);

-- COMMAND ----------

create
or replace table silver.cash_transactions as (
  with t as (
    select
      ct_ca_id account_id,
      ct_dts transaction_timestamp,
      ct_amt amount,
      ct_name description
    from
      bronze.brokerage_cash_transaction
  )
  select
    a.customer_id,
    t.*
  from
    t
    join silver.accounts a on t.account_id = a.account_id
    and t.transaction_timestamp between a.effective_timestamp
    and a.end_timestamp
);

-- COMMAND ----------

create
or replace table silver.companies as (
  select
    cik as company_id,
    st.st_name status,
    company_name name,
    ind.in_name industry,
    ceo_name ceo,
    address_line1,
    address_line2,
    postal_code,
    city,
    state_province,
    country,
    description,
    founding_date,
    sp_rating,
    pts as effective_timestamp,
    ifnull(
      timestampadd(
        millisecond,
        -1,
        lag(pts) over (
          partition by cik
          order by
            pts desc
        )
      ),
      to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    CASE
      WHEN (
        row_number() over (
          partition by cik
          order by
            pts desc
        ) = 1
      ) THEN TRUE
      ELSE FALSE
    END as IS_CURRENT
  from
    bronze.finwire_company cmp
    join bronze.reference_status_type st on cmp.status = st.st_id
    join bronze.reference_industry ind on cmp.industry_id = ind.in_id
);

-- COMMAND ----------

create
or replace table silver.customers as (
  select
    action_type,
    decode(
      action_type,
      'NEW',
      'Active',
      'ADDACCT',
      'Active',
      'UPDACCT',
      'Active',
      'UPDCUST',
      'Active',
      'INACT',
      'Inactive'
    ) status,
    c_id customer_id,
    ca_id account_id,
    c_tax_id tax_id,
    c_gndr gender,
    c_tier tier,
    c_dob dob,
    c_l_name last_name,
    c_f_name first_name,
    c_m_name middle_name,
    c_adline1 address_line1,
    c_adline2 address_line2,
    c_city city,
    c_state_prov state_province,
    c_zipcode postal_code,
    c_ctry country,
    c_prim_email primary_email,
    c_alt_email alternate_email,
    c_phone_1 phone1,
    c_phone_2 phone2,
    c_phone_3 phone3,
    c_lcl_tx_id local_tax_rate_name,
    ltx.tx_rate local_tax_rate,
    c_nat_tx_id national_tax_rate_name,
    ntx.tx_rate national_tax_rate,
    ca_tax_st account_tax_status,
    ca_b_id broker_id,
    action_ts as effective_timestamp,
    ifnull(
      timestampadd(
        MILLISECOND,
        -1,
        lag(action_ts) over (
          partition by c_id
          order by
            action_ts desc
        )
      ),
      to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    CASE
      WHEN (
        row_number() over (
          partition by c_id
          order by
            action_ts desc
        ) = 1
      ) THEN TRUE
      ELSE FALSE
    END as IS_CURRENT
  from
    bronze.crm_customer_mgmt c
    left join bronze.reference_tax_rate ntx on c.c_nat_tx_id = ntx.tx_id
    left join bronze.reference_tax_rate ltx on c.c_lcl_tx_id = ltx.tx_id
  where
    action_type in ('NEW', 'INACT', 'UPDCUST')
);

-- COMMAND ----------

create
or replace table silver.daily_market as (
  with s1 as (
    select
      -- dm_date,
      min(dm_low) over (
        partition by dm_s_symb
        order by
          dm_date asc rows between 364 preceding
          and 0 following -- CURRENT ROW
      ) fifty_two_week_low,
      max(dm_high) over (
        partition by dm_s_symb
        order by
          dm_date asc rows between 364 preceding
          and 0 following -- CURRENT ROW
      ) fifty_two_week_high,
      *
    from
      bronze.brokerage_daily_market
  ),
  s2 as (
    select
      a.*,
      b.dm_date as fifty_two_week_low_date,
      c.dm_date as fifty_two_week_high_date
    from
      s1 a
      join s1 b on a.dm_s_symb = b.dm_s_symb
      and a.fifty_two_week_low = b.dm_low
      and b.dm_date between add_months(a.dm_date, -12)
      and a.dm_date
      join s1 c on a.dm_s_symb = c.dm_s_symb
      and a.fifty_two_week_high = c.dm_high
      and c.dm_date between add_months(a.dm_date, -12)
      and a.dm_date
  )
  select
    *
  from
    s2 qualify row_number() over (
      partition by dm_s_symb,
      dm_date
      order by
        fifty_two_week_low_date,
        fifty_two_week_high_date
    ) = 1
);

-- COMMAND ----------

create
or replace table silver.date as (
  select
    *
  from
    bronze.reference_date
);

-- COMMAND ----------

create
or replace table silver.employees as (
  select
    employee_id,
    manager_id,
    employee_first_name first_name,
    employee_last_name last_name,
    employee_mi middle_initial,
    employee_job_code job_code,
    employee_branch branch,
    employee_office office,
    employee_phone phone
  from
    bronze.hr_employee
);

-- COMMAND ----------

create
or replace table silver.financials as (
  with s1 as (
    select
      YEAR,
      QUARTER,
      QUARTER_START_DATE,
      POSTING_DATE,
      REVENUE,
      EARNINGS,
      EPS,
      DILUTED_EPS,
      MARGIN,
      INVENTORY,
      ASSETS,
      LIABILITIES,
      SH_OUT,
      DILUTED_SH_OUT,
      coalesce(c1.name, c2.name) company_name,
      coalesce(c1.company_id, c2.company_id) company_id,
      pts as effective_timestamp
    from
      bronze.finwire_financial s
      left join silver.companies c1 on s.cik = c1.company_id
      and pts between c1.effective_timestamp
      and c1.end_timestamp
      left join silver.companies c2 on s.company_name = c2.name
      and pts between c2.effective_timestamp
      and c2.end_timestamp
  )
  select
    *,
    ifnull(
      timestampadd(
        millisecond,
        -1,
        lag(effective_timestamp) over (
          partition by company_id
          order by
            effective_timestamp desc
        )
      ),
      to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    CASE
      WHEN (
        row_number() over (
          partition by company_id
          order by
            effective_timestamp desc
        ) = 1
      ) THEN TRUE
      ELSE FALSE
    END as IS_CURRENT
  from
    s1
);

-- COMMAND ----------

create
or replace table silver.trades_history as (
  select
    t_id trade_id,
    t_dts trade_timestamp,
    t_ca_id account_id,
    ts.st_name trade_status,
    tt_name trade_type,
    case
      t_is_cash
      when true then 'Cash'
      when false then 'Margin'
    end transaction_type,
    t_s_symb symbol,
    t_exec_name executor_name,
    t_qty quantity,
    t_bid_price bid_price,
    t_trade_price trade_price,
    t_chrg fee,
    t_comm commission,
    t_tax tax,
    us.st_name update_status,
    th_dts effective_timestamp,
    ifnull(
      timestampadd(
        millisecond,
        -1,
        lag(th_dts) over (
          partition by t_id
          order by
            th_dts desc
        )
      ),
      to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    CASE
      WHEN (
        row_number() over (
          partition by t_id
          order by
            th_dts desc
        ) = 1
      ) THEN TRUE
      ELSE FALSE
    END as IS_CURRENT
  from
    bronze.brokerage_trade
    join bronze.brokerage_trade_history on t_id = th_t_id
    join bronze.reference_trade_type on t_tt_id = tt_id
    join bronze.reference_status_type ts on t_st_id = ts.st_id
    join bronze.reference_status_type us on th_st_id = us.st_id
);

-- COMMAND ----------

create
or replace table silver.trades as (
  with s1 as (
    select
      distinct trade_id,
      account_id,
      trade_status,
      trade_type,
      transaction_type,
      symbol,
      executor_name,
      quantity,
      bid_price,
      trade_price,
      fee,
      commission,
      tax,
      min(effective_timestamp) over (partition by trade_id) create_timestamp,
      max(effective_timestamp) over (partition by trade_id) close_timestamp
    from
      silver.trades_history
    order by
      trade_id,
      create_timestamp
  )
  select
    *
  from
    s1
);

-- COMMAND ----------

create
or replace table silver.holdings_history as (
  with s1 as (
    select
      HH_T_ID trade_id,
      HH_H_T_ID previous_trade_id,
      hh_before_qty previous_quantity,
      hh_after_qty quantity
    from
      bronze.brokerage_holding_history
  )
  select
    s1.*,
    ct.account_id account_id,
    ct.symbol symbol,
    ct.create_timestamp,
    ct.close_timestamp,
    ct.trade_price,
    ct.bid_price,
    ct.fee,
    ct.commission
  from
    s1
    join silver.trades ct using (trade_id)
);

-- COMMAND ----------

create
or replace table silver.securities as (
  select
    symbol,
    issue_type,
    case
      s.status
      when 'ACTV' then 'Active'
      when 'INAC' then 'Inactive'
      else null
    end status,
    s.name,
    ex_id exchange_id,
    sh_out shares_outstanding,
    first_trade_date,
    first_exchange_date,
    dividend,
    coalesce(c1.name, c2.name) company_name,
    coalesce(c1.company_id, c2.company_id) company_id,
    pts as effective_timestamp,
    ifnull(
      timestampadd(
        millisecond,
        -1,
        lag(pts) over (
          partition by symbol
          order by
            pts desc
        )
      ),
      to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    CASE
      WHEN (
        row_number() over (
          partition by symbol
          order by
            pts desc
        ) = 1
      ) THEN TRUE
      ELSE FALSE
    END as IS_CURRENT
  from
    bronze.finwire_security s
    left join silver.companies c1 on s.cik = c1.company_id
    and pts between c1.effective_timestamp
    and c1.end_timestamp
    left join silver.companies c2 on s.company_name = c2.name
    and pts between c2.effective_timestamp
    and c2.end_timestamp
);

-- COMMAND ----------

create
or replace table silver.watches_history as (
  with s1 as (
    select
      w_c_id customer_id,
      w_s_symb symbol,
      w_dts watch_timestamp,
      case
        w_action
        when 'ACTV' then 'Activate'
        when 'CNCL' then 'Cancelled'
        else null
      end action_type
    from
      bronze.brokerage_watch_history
  )
  select
    s1.*,
    company_id,
    company_name,
    exchange_id,
    status security_status
  from
    s1
    join silver.securities s using (symbol)
);

-- COMMAND ----------

create
or replace  table silver.watches as (
  with s1 as (
    select
      customer_id,
      symbol,
      watch_timestamp,
      action_type,
      company_id,
      company_name,
      exchange_id,
      security_status,
      case
        action_type
        when 'Activate' then watch_timestamp
        else null
      end placed_timestamp,
      case
        action_type
        when 'Cancelled' then watch_timestamp
        else null
      end removed_timestamp
    from
      silver.watches_history
  ),
  s2 as (
    select
      customer_id,
      symbol,
      company_id,
      company_name,
      exchange_id,
      security_status,
      min(placed_timestamp) placed_timestamp,
      max(removed_timestamp) removed_timestamp
    from
      s1
    group by
      all
  )
  select
    *,
    case
      when removed_timestamp is null then 'Active'
      else 'Inactive'
    end watch_status
  from
    s2
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Load the dimension tables in the Gold zone.

-- COMMAND ----------

create
or replace table gold.dim_customer as (
  with s1 as (
    select
      c.*,
      p.agency_id,
      p.credit_rating,
      p.net_worth
    FROM
      silver.customers c
      left join bronze.syndicated_prospect p using (
        first_name,
        last_name,
        postal_code,
        address_line1,
        address_line2
      )
  ),
  s2 as (
    SELECT
      md5(
        cast(
          coalesce(
            cast(customer_id as STRING),
            '_utils_surrogate_key_null_'
          ) || '-' || coalesce(
            cast(effective_timestamp as STRING),
            '_utils_surrogate_key_null_'
          ) as STRING
        )
      ) sk_customer_id,
      customer_id,
      coalesce(
        tax_id,
        last_value(tax_id) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) tax_id,
      status,
      coalesce(
        last_name,
        last_value(last_name) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) last_name,
      coalesce(
        first_name,
        last_value(first_name) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) first_name,
      coalesce(
        middle_name,
        last_value(middle_name) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) middleinitial,
      coalesce(
        gender,
        last_value(gender) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) gender,
      coalesce(
        tier,
        last_value(tier) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) tier,
      coalesce(
        dob,
        last_value(dob) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) dob,
      coalesce(
        address_line1,
        last_value(address_line1) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) address_line1,
      coalesce(
        address_line2,
        last_value(address_line2) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) address_line2,
      coalesce(
        postal_code,
        last_value(postal_code) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) postal_code,
      coalesce(
        CITY,
        last_value(CITY) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) CITY,
      coalesce(
        state_province,
        last_value(state_province) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) state_province,
      coalesce(
        country,
        last_value(country) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) country,
      coalesce(
        phone1,
        last_value(phone1) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) phone1,
      coalesce(
        phone2,
        last_value(phone2) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) phone2,
      coalesce(
        phone3,
        last_value(phone3) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) phone3,
      coalesce(
        primary_email,
        last_value(primary_email) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) primary_email,
      coalesce(
        alternate_email,
        last_value(alternate_email) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) alternate_email,
      coalesce(
        local_tax_rate_name,
        last_value(local_tax_rate_name) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) local_tax_rate_name,
      coalesce(
        local_tax_rate,
        last_value(local_tax_rate) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) local_tax_rate,
      coalesce(
        national_tax_rate_name,
        last_value(national_tax_rate_name) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) national_tax_rate_name,
      coalesce(
        national_tax_rate,
        last_value(national_tax_rate) IGNORE NULLS OVER (
          PARTITION BY customer_id
          ORDER BY
            effective_timestamp
        )
      ) national_tax_rate,
      agency_id,
      credit_rating,
      net_worth,
      effective_timestamp,
      end_timestamp,
      is_current
    FROM
      s1
  )
  select
    *
  from
    s2
);

-- COMMAND ----------

create
or replace table gold.dim_account as (
  SELECT
    md5(
      cast(
        coalesce(
          cast(account_id as string),
          '_utils_surrogate_key_null_'
        ) || '-' || coalesce(
          cast(a.effective_timestamp as string),
          '_utils_surrogate_key_null_'
        ) as string
      )
    ) sk_account_id,
    a.account_id,
    sk_broker_id,
    sk_customer_id,
    a.status,
    account_desc,
    tax_status,
    a.effective_timestamp,
    a.end_timestamp,
    a.is_current
  from
    silver.accounts a
    join gold.dim_customer c on a.customer_id = c.customer_id
    and a.effective_timestamp between c.effective_timestamp
    and c.end_timestamp
    join gold.dim_broker b using (broker_id)
);

-- COMMAND ----------

create
or replace table gold.dim_broker as (
  select
    md5(
      cast(
        coalesce(
          cast(employee_id as STRING),
          '_utils_surrogate_key_null_'
        ) as STRING
      )
    ) sk_broker_id,
    employee_id broker_id,
    manager_id,
    first_name,
    last_name,
    middle_initial,
    job_code,
    branch,
    office,
    phone
  from
    silver.employees
);

-- COMMAND ----------

create
or replace table gold.dim_company as (
  select
    md5(
      cast(
        coalesce(
          cast(company_id as STRING),
          '_utils_surrogate_key_null_'
        ) || '-' || coalesce(
          cast(effective_timestamp as STRING),
          '_utils_surrogate_key_null_'
        ) as STRING
      )
    ) sk_company_id,
    company_id,
    status,
    name,
    industry,
    ceo,
    address_line1,
    address_line2,
    postal_code,
    city,
    state_province,
    country,
    description,
    founding_date,
    sp_rating,
    case
      when sp_rating in (
        'BB',
        'B',
        'CCC',
        'CC',
        'C',
        'D',
        'BB+',
        'B+',
        'CCC+',
        'BB-',
        'B-',
        'CCC-'
      ) then true
      else false
    end as is_lowgrade,
    effective_timestamp,
    end_timestamp,
    is_current
  from
    silver.companies
);

-- COMMAND ----------

create
or replace table gold.dim_date as (
  select
    *
  from
    silver.date
);

-- COMMAND ----------

create
or replace table gold.dim_security as (
  with s1 as (
    select
      symbol,
      issue_type issue,
      s.status,
      s.name,
      exchange_id,
      sk_company_id,
      shares_outstanding,
      first_trade_date,
      first_exchange_date,
      dividend,
      s.effective_timestamp,
      s.end_timestamp,
      s.IS_CURRENT
    from
      silver.securities s
      join gold.dim_company c on s.company_id = c.company_id
      and s.effective_timestamp between c.effective_timestamp
      and c.end_timestamp
  )
  select
    md5(
      cast(
        coalesce(
          cast(symbol as STRING),
          '_utils_surrogate_key_null_'
        ) || '-' || coalesce(
          cast(effective_timestamp as STRING),
          '_utils_surrogate_key_null_'
        ) as STRING
      )
    ) sk_security_id,
    *
  from
    s1
);

-- COMMAND ----------

create
or replace table gold.dim_trade as (
  select
    md5(
      cast(
        coalesce(
          cast(trade_id as STRING),
          '_utils_surrogate_key_null_'
        ) || '-' || coalesce(
          cast(t.effective_timestamp as STRING),
          '_utils_surrogate_key_null_'
        ) as STRING
      )
    ) sk_trade_id,
    trade_id,
    trade_status status,
    transaction_type,
    trade_type type,
    executor_name executed_by,
    t.effective_timestamp,
    t.end_timestamp,
    t.IS_CURRENT
  from
    silver.trades_history t
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create the fact tables in the Gold zone.

-- COMMAND ----------

create
or replace table gold.fact_market_history as (
  with s1 as (
    select
      sk_company_id,
      f.company_id,
      QUARTER_START_DATE,
      sum(eps) over (
        partition by f.company_id
        order by
          QUARTER_START_DATE rows between 4 preceding
          and current row
      ) - eps sum_basic_eps
    from
      silver.financials f
      join gold.dim_company c on f.company_id = c.company_id
      and f.effective_timestamp between c.effective_timestamp
      and c.end_timestamp
  )
  SELECT
    s.sk_security_id,
    s.sk_company_id,
    dm_date sk_date_id,
    --dmh.dm_close / sum_basic_eps AS peratio,
    (s.dividend / dmh.dm_close) / 100 yield,
    fifty_two_week_high,
    fifty_two_week_high_date sk_fifty_two_week_high_date,
    fifty_two_week_low,
    fifty_two_week_low_date sk_fifty_two_week_low_date,
    dm_close closeprice,
    dm_high dayhigh,
    dm_low daylow,
    dm_vol volume
  FROM
    silver.daily_market dmh
    JOIN gold.dim_security s ON s.symbol = dmh.dm_s_symb
    AND dmh.dm_date between s.effective_timestamp
    and s.end_timestamp
    LEFT JOIN s1 f USING (sk_company_id)
);

-- COMMAND ----------

create
or replace table gold.fact_trade as (
  select
    sk_trade_id,
    sk_broker_id,
    sk_customer_id,
    sk_account_id,
    sk_security_id,
    to_date(create_timestamp) sk_create_date,
    create_timestamp,
    to_date(close_timestamp) sk_close_date,
    close_timestamp,
    executed_by,
    quantity,
    bid_price,
    trade_price,
    fee,
    commission,
    tax
  from
    silver.trades t
    join gold.dim_trade dt on t.trade_id = dt.trade_id
    and t.create_timestamp between dt.effective_timestamp
    and dt.end_timestamp
    join gold.dim_account a on t.account_id = a.account_id
    and t.create_timestamp between a.effective_timestamp
    and a.end_timestamp
    join gold.dim_security s on t.symbol = s.symbol
    and t.create_timestamp between s.effective_timestamp
    and s.end_timestamp
);

-- COMMAND ----------

create
or replace table gold.fact_watches as (
  select
    sk_customer_id,
    sk_security_id,
    to_date(placed_timestamp) sk_date_placed,
    to_date(removed_timestamp) sk_date_removed,
    1 as watch_cnt
  from
    silver.watches w
    join gold.dim_customer c ON w.customer_id = c.customer_id
    and placed_timestamp between c.effective_timestamp
    and c.end_timestamp
    join gold.dim_security s ON w.symbol = s.symbol
    and placed_timestamp between s.effective_timestamp
    and s.end_timestamp
);

-- COMMAND ----------

create
or replace table gold.fact_cash_transactions as (
  with s1 as (
    select
      *,
      to_date(transaction_timestamp) sk_transaction_date
    from
      silver.cash_transactions
  )
  select
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    transaction_timestamp,
    amount,
    description
  from
    s1
    join gold.dim_account a on s1.account_id = a.account_id
    and s1.transaction_timestamp between a.effective_timestamp
    and a.end_timestamp
);

-- COMMAND ----------

create
or replace table gold.fact_cash_balances as (
  with s1 as (
    select
      *
    from
      gold.fact_cash_transactions
  )
  select
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    sum(amount) amount,
    description
  from
    s1
  group by
    all
  order by
    sk_transaction_date,
    sk_customer_id,
    sk_account_id
);

-- COMMAND ----------

create
or replace table gold.fact_holdings as (
  with s1 as (
    select
      *
    from
      silver.holdings_history
  )
  select
    ct.sk_trade_id sk_current_trade_id,
    pt.sk_trade_id,
    sk_customer_id,
    sk_account_id,
    sk_security_id,
    to_date(create_timestamp) sk_trade_date,
    create_timestamp trade_timestamp,
    trade_price current_price,
    quantity current_holding,
    bid_price current_bid_price,
    fee current_fee,
    commission current_commission
  from
    s1
    join gold.dim_trade ct using (trade_id)
    join gold.dim_trade pt on s1.previous_trade_id = pt.trade_id
    join gold.dim_account a on s1.account_id = a.account_id
    and s1.create_timestamp between a.effective_timestamp
    and a.end_timestamp
    join gold.dim_security s on s1.symbol = s.symbol
);
