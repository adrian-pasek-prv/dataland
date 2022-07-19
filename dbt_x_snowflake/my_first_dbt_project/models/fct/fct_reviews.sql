{{
    config(
        materialized = 'incremental',
        on_schema_change = 'fail'
    )
}}
-- materialized = 'incremental' -> append rows in this model
-- on_schema_change = 'fail' -> since table is not recreated, the schema is set and if it changes somehow we need to react
WITH src_reviews AS (
    SELECT * FROM {{ ref('src_reviews') }}
)

SELECT {{ dbt_utils.surrogate_key(['listing_id', 'review_date', 'reviewer_name', 'review_text']) }} as review_id,
        *
FROM src_reviews
WHERE review_text IS NOT NULL
-- if materialized is incremental then add AND clause which states that retrive only rows where review_date is after max(review_date) of the model {{ this }}
{% if is_incremental() %}
    AND review_date > (SELECT MAX(review_date) FROM {{ this }})
{% endif %}