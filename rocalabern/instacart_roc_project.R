# init ----
spark_memory = "2G"
source("readDataToMemory.R")
readInstacart()

library(DBI)
library(ggplot2)
library(ggthemes)

# exploration ---- 

# Si l'has comprat mes de n vegades (different tickets) els ultims dos meses respecte la ultima compra 1
# Sino, weight=promig aparicions per compra

dbGetQuery(sc, 
"
SELECT
  s.user_id,
  s.product_id,
  CASE WHEN s.num > 10 THEN 10 ELSE s.num END as num
FROM (
  SELECT
    t.user_id,
    t.product_id,
    count(1) as num
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
    GROUP BY
      op.order_id,
      op.product_id,
      o.user_id
    JOIN item_factors_tbl f ON f.id = p.product_id
    ) t
) s
")

user_item_rating <- order_products__prior %>%
  select(order_id, product_id) %>%
  left_join(orders, by="order_id") %>%
  filter(user_id <= 50) %>% 
  select(product_id, user_id) %>%
  group_by(user_id, product_id) %>%
  summarise(rating = n()) %>%
  rename(user = user_id) %>%
  mutate(item=product_id) %>%
  select(user, item, rating)

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

