sel_product_id = 46979
sel_product_id = 27876

df_sel_product = dbGetQuery(sc, whisker::whisker.render("
SELECT 
  op.product_id,   
  n_orders,
  product_name
FROM (
    SELECT 
      product_id,   
      COUNT(distinct order_id) AS n_orders
    FROM 
        order_products__prior_tbl
    WHERE
      product_id == {{sel_product_id}}
    GROUP BY 
        product_id
    ORDER BY 
        n_orders DESC
    ) op
  LEFT JOIN products_tbl p ON op.product_id = p.product_id
WHERE 
  n_orders > 100
"), list(sel_product_id=sel_product_id))
df_sel_product

dbGetQuery(sc, "DROP TABLE IF EXISTS contains_product")
dbGetQuery(sc, whisker::whisker.render("
CREATE TABLE contains_product AS
  SELECT
    order_id,
    max(case when (product_id = {{sel_product_id}}) then 1 else 0 end) as ind_product
  FROM 
    order_products__prior_tbl
  GROUP BY
    order_id
"), list(sel_product_id=sel_product_id))
dbGetQuery(sc, "SELECT SUM(ind_product) as num_product, count(distinct order_id) as num_total, count(1) as num_total_rep from contains_product")

dbGetQuery(sc, "DROP TABLE IF EXISTS ratio_product")
dbGetQuery(sc, whisker::whisker.render("
CREATE TABLE ratio_product AS
  select 
    product_id,
    count(distinct o.order_id) num_times,
    count(1) num_times_rep,
    count(distinct o.order_id)/count(1) rep_product,
    count(distinct o.order_id)/3214874 as ratio_product,
    ({{n_orders}}/3214874)*(count(distinct o.order_id)/3214874) as ratio_product_independent,
    sum(c.ind_product)/3214874 as ratio_product_real,
    (sum(c.ind_product)/3214874)/(({{n_orders}}/3214874)*(count(distinct o.order_id)/3214874)) as lift
  from 
    order_products__prior_tbl o
      JOIN contains_product c ON c.order_id = o.order_id
  group by 
    product_id
", list(n_orders = df_sel_product$n_orders)))

dbGetQuery(sc, "DROP TABLE IF EXISTS lift_high")
dbGetQuery(sc, "
CREATE TABLE lift_high AS
  SELECT
    r.*
  FROM
    ratio_product r
  WHERE
    r.num_times >100
  ORDER BY
    r.lift desc
  limit 1000
")

dbGetQuery(sc, "DROP TABLE IF EXISTS lift_low")
dbGetQuery(sc, "
CREATE TABLE lift_low AS
  SELECT
    r.*
  FROM
    ratio_product r
  WHERE
    r.num_times > 200
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

df_lift_high[1:10,]
df_lift_low[1:10,]
