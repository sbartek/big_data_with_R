# init ----
spark_memory = "2G"
source("readDataToMemory.R")
readInstacart()

library(DBI)
library(ggplot2)
library(ggthemes)
library(data.table)

# exploration most bought ---- 
products_popularity_txt <- "
SELECT op.product_id
,   n_orders
,   product_name
FROM (
    SELECT product_id
    ,   COUNT(1) AS n_orders
    FROM order_products__prior_tbl
    GROUP BY product_id
    ORDER BY n_orders DESC
    LIMIT 30) op
LEFT JOIN (
    SELECT product_id
    ,   product_name
    FROM products_tbl) p
ON op.product_id = p.product_id
"
df_most_bought = dbGetQuery(sc, products_popularity_txt) %>% as.data.table
df_most_bought

# exploration correlation ---- 

dbGetQuery(sc, "
CREATE TABLE contains_banana AS
  SELECT
    order_id,
    max(case when (product_id = 24852 OR product_id = 13176) then 1 else 0 end) as ind_banana
  FROM 
    order_products__prior_tbl
  GROUP BY
    order_id
")

dbGetQuery(sc, "
  select 
    ind_banana, count(1) as num
  from 
    contains_banana
  group by 
    ind_banana
")

df_most_bought

dbGetQuery(sc, "
SELECT
  count(distinct order_id) as num 
FROM
    order_products__prior_tbl o
")

dbGetQuery(sc, "DROP TABLE IF EXISTS ratio_product")
dbGetQuery(sc, "
CREATE TABLE ratio_product AS
  select 
    product_id,
    count(1)/3214874 as ratio_product,
    (850839/3214874)*(count(1)/3214874) as ratio_product_banana_independent,
    sum(c.ind_banana)/3214874 as ratio_product_banana_real,
    (sum(c.ind_banana)/3214874)/((850839/3214874)*(count(1)/3214874)) as lift
  from 
    order_products__prior_tbl o
      JOIN contains_banana c ON c.order_id = o.order_id
  group by 
    product_id
")

dbGetQuery(sc, "
CREATE TABLE lift_high AS
  SELECT
    r.*
  FROM
    ratio_product r
  ORDER BY
    r.lift desc
  limit 1000
")

dbGetQuery(sc, "
CREATE TABLE lift_low AS
  SELECT
    r.*
  FROM
    ratio_product r
  ORDER BY
    r.lift asc
  limit 1000
")

df_lift_high = dbGetQuery(sc, "
SELECT
  p.product_name,
  l.*
FROM
  lift_high l
  LEFT JOIN products_tbl p ON p.product_id = l.product_id
ORDER BY
  l.lift desc
")
df_lift_high

df_lift_low = dbGetQuery(sc, "
SELECT
  p.product_name,
  l.*
FROM
  lift_low l
  LEFT JOIN products_tbl p ON p.product_id = l.product_id
ORDER BY
  l.lift asc
")
df_lift_low