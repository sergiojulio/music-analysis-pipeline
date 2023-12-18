
-- Use the `ref` function to select from other models

{{ config(materialized='table') }}

SELECT 
    id, name, popularity, followers
FROM 
    {{ var("artists")  }} AS artists
LIMIT
    100
