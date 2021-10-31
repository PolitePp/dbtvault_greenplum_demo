WITH staging AS (
select generate_series::date as AS_OF_DATE
from generate_series(date'1992-01-08', date'1992-01-11', interval '1 day')
)

SELECT *
FROM staging