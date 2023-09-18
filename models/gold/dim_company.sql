select
    {{dbt_utils.generate_surrogate_key(['company_id','effective_timestamp'])}} sk_company_id,
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
        when
            sp_rating in (
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
            )
        then true
        else false
    end as is_lowgrade,
    effective_timestamp,
    end_timestamp,
    is_current
from {{ ref("companies") }}