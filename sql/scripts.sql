--распределение событий по часам;
select extract('HOUR'from event_timestamp)::int as hours , replace(page_url_path,'/','') as event_type,
count(replace(page_url_path,'/',''))
from staging.events
group by 1,2;

SELECT count(x.*), 
extract('hour' from x.event_timestamp) AS hour
FROM staging.events x
GROUP BY 2;

--количество купленных товаров в разрезе часа;
select extract('HOUR'from event_timestamp)::int as hours, user_custom_id, count(event_id)
from  staging.events
where replace(page_url_path,'/','') = 'confirmation'
group by 1,2;

--топ-10 посещённых страниц, с которых был переход в покупку — список ссылок с количеством покупок.
with a as (select page_url_path,lag(page_url_path) OVER (PARTITION BY user_custom_id  ORDER BY event_timestamp) AS last_page
from staging.events)
select page_url_path, count(last_page)
from a 
group by 1
order by 2 desc
limit 10;
