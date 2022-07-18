WITH d AS (
    SELECT * FROM {{ ref('dim_listings_cleansed') }}
),
f AS (
    SELECT * FROM {{ ref('fct_reviews') }}
)

SELECT *
FROM    d
JOIN    f ON d.listing_id=f.listing_id
WHERE   f.review_date < d.created_at