# init ----
spark_memory = "2G"
source("readDataToMemory.R")
readInstacart()

library(DBI)
library(ggplot2)
library(ggthemes)

dbGetQuery(sc, "DROP TABLE IF EXISTS products_most_bought")
dbGetQuery(sc, "
CREATE TABLE products_most_bought AS
SELECT 
  op.product_id,   
  n_orders,
  product_name
FROM (
    SELECT 
      product_id,   
      COUNT(1) AS n_orders
    FROM 
        order_products__prior_tbl
    GROUP BY 
        product_id
    ORDER BY 
        n_orders DESC
    ) op
  LEFT JOIN products_tbl p ON op.product_id = p.product_id
WHERE 
  n_orders > 100
")

dbGetQuery(sc, "DROP TABLE IF EXISTS rating_table")
dbGetQuery(sc, "
CREATE TABLE rating_table AS
SELECT
  t.user_id,
  t.product_id,
  CASE WHEN count(1) > 10 THEN 10 ELSE s.num END as rating,
  count(1) as num,
  count(distinct order_id) as num_orders
FROM
  (
  SELECT 
    op.order_id,
    op.product_id,
    o.user_id,
    1 as num
  FROM 
    order_products__prior_tbl op
      LEFT JOIN orders_tbl o ON o.order_id = op.order_id
      JOIN products_most_bought p ON p.product_id = op.product_id
  GROUP BY
    op.order_id,
    op.product_id,
    o.user_id
  JOIN item_factors_tbl f ON f.id = p.product_id
  ) t
")

dbGetQuery(sc, "DROP TABLE IF EXISTS rating_table_filter")
dbGetQuery(sc, "
CREATE TABLE rating_table_filter AS
SELECT
  t.*
FROM 
  rating_table t
WHERE

")

user_item_rating = dbGetQuery(sc, "SELECT * FROM rating_table_filter")

explicit_model <- ml_als_factorization(user_item_rating, rank = 2, iter.max = 5, regularization.parameter = 0.01)

item_factors <- copy_to(sc, explicit_model$item.factors, "item_factors_tbl", overwrite = TRUE)

df_embedding = dbGetQuery(sc, 
"
SELECT 
  p.product_name,
  f.*
FROM 
products_tbl p
JOIN item_factors_tbl f ON f.id = p.product_id
")

kmeans_model <- item_factors %>%
  ml_kmeans(centers = 30)

kmeans_predicted <- sdf_predict(kmeans_model, item_factors) %>% collect()

for (cluster in sort(unique(kmeans_predicted$prediction))) {
  list_product_id_cluster = kmeans_predicted$id[kmeans_predicted$prediction==cluster]
  message("------------------------------------------------")
  message(paste0(head(df_embedding$product_name[df_embedding$id %in% list_product_id_cluster],20), collapse = " | "))
}
table(predicted$Species, predicted$prediction)
kmeans_model$
# print our model fit
print(kmeans_model)

# ggplot(df_embedding) + geom_text(aes(x=V1, y=V2, label=product_name))
ggplot(df_embedding[sample(nrow(df_embedding), 100),]) + geom_text(aes(x=V1, y=V2, label=product_name), size=3.0, check_overlap=TRUE)

# http://www.sthda.com/english/wiki/ggplot2-texts-add-text-annotations-to-a-graph-in-r-software
ggplot(df_embedding) + ggrepel::geom_text_repel(aes(x=V1, y=V2, label=product_name))

ggplot(df_embedding) + ggrepel::geom_label_repel(aes(x=V1, y=V2, label=product_name))

