{% snapshot scd_raw_listings %}
-- scd_raw_listings is the table name where snapshots will be stored

{{
    config(
        target_schema='dev',
        unique_key='id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}
-- [target_schema] - target schema where snapshot table will be stored
-- [strategy = 'timestamp'] - take a look at combination of unique key and timestamp to track changes
-- [invalidate_hard_deletes = True] - pick up any delete action of the record

SELECT * FROM {{ source('airbnb', 'listings') }}

{% endsnapshot %}
