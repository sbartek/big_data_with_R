source('configuration.R')

library(sparklyr)
library(RSQLite)
library(dplyr)

config <- spark_config()
config$`sparklyr.shell.driver-memory` <- "4G"
config$`sparklyr.shell.executor-memory` <- "4G"
config$`spark.yarn.executor.memoryOverhead` <- "1G"
sc <- spark_connect(master = "local", config = config)
#sc <- spark_connect(master = "local")

order_products__prior <- NULL
order_products__train <- NULL
orders <- NULL
products <- NULL

readInstacart <- function() {
  order_products__prior <<- 
    spark_read_csv(sc, "order_products__prior_tbl", 
                   file.path(DATA_DIR, "order_products__prior.csv"))
  
  order_products__train <<- 
    spark_read_csv(sc, "order_products__train_tbl", 
                   file.path(DATA_DIR, "order_products__train.csv"))
  
  orders <<- spark_read_csv(sc, "orders_tbl", file.path(DATA_DIR, "orders.csv"))
  
  products <<- spark_read_csv(sc, "products_tbl", file.path(DATA_DIR, "products.csv"))
}


players <- NULL
countries <- NULL
sql_con <- NULL

readFootball <- function() {
  sql_con <<- dbConnect(
    SQLite(), 
    dbname=file.path(DATA_DIR, "database.sqlite")
  )
  players <<- tbl_df(dbGetQuery(sql_con,"SELECT * FROM Player"))
  countries <<- tbl_df(dbGetQuery(sql_con,"SELECT * FROM Country"))
  games <<- tbl_df(dbGetQuery(sql_con,"SELECT * FROM Match"))
  
  #dbDisconnect(sql_con)
}



