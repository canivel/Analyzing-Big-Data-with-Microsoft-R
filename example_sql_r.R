setwd('F:/MicrosoftR/edx')
options(max.print = 1000, scipen = 999, width = 90)
library(RevoScaleR)
rxOptions(reportProgress = 1)
library(dplyr)
options(dplyr.print_max = 2000)
options(dplyr.width = Inf)
library(stringr)
library(lubridade)
library(sp)
library(maptools)
library(ggmap)
library(ggplot2)
library(gridExtra)
library(ggrepel)
library(tidyr)
library(seriation)

sqlConnString <- "Driver=SQL Server;Server=SETHMOTTDSVM;Database=RDB;Uid=ruser;Pwd=ruser"
sqlRowsPerRead <- 100000
sqlTable <- "NYCTaxiBig"

nyc_sql <- RxSqlServerData(connectionString = sqlConnString, rowsPerRead = sqlRowsPerRead, table = sqlTable)

system.time(
  rxDataStep(nyc_xdf, nyc_sql, overwrite = TRUE)
)

ccColInfo <- list(
  tpep_pickup_datetime = list(type = "character"),
  tpep_dropoff_datetime = list(type = "character"),
  passenger_count = list(type = "integer"),
  trip_distance = list(type = "numeric"),
  pickup_longitude = list(type = "numeric"),
  pickup_latitude = list(type = "numeric"),
  dropoff_longitude = list(type = "numeric"),
  dropoff_latitude = list(type = "numeric"),
  RateCodeID = list(type = "factor", levels = as.character(1:6), newLevels = c("standard", "JFK", "Newark", "Nassau or Westchester", "negotiated", "group ride")),
  store_and_fwd_flag = list(type = "factor", levels = c("Y", "N")),
  payment_type = list(type = "factor", levels = as.character(1:2), newLevels = c("card", "cash")),
  fare_amount = list(type = "numeric"),
  tip_amount = list(type = "numeric"),
  total_amount = list(type = "numeric")
)

weekday_labels <- c('Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat')
hour_labels <- c('1AM-5AM', '5AM-9AM', '9AM-12PM', '12PM-4PM', '4PM-6PM', '6PM-10PM', '10PM-1AM')

ccColInfo$pickup_dow <- list(type = "factor", levels = weekday_labels)
ccColInfo$pickup_hour <- list(type = "factor", levels = hour_labels)
ccColInfo$dropoff_dow <- list(type = "factor", levels = weekday_labels)
ccColInfo$dropoff_hour <- list(type = "factor", levels = hour_labels)

nyc_shapefile <- readShapePoly('ZillowNeighborhoods-NY/ZillowNeighborhoods-NY.shp')
mht_shapefile <- subset(nyc_shapefile, str_detect(CITY, 'New York City-Manhattan'))
manhattan_nhoods <- as.character(mht_shapefile@data$NAME)

ccColInfo$pickup_nhood <- list(type = "factor", levels = manhattan_nhoods)
ccColInfo$dropoff_nhood <- list(type = "factor", levels = manhattan_nhoods)

nyc_sql <- RxSqlServerData(connectionString = sqlConnString, table = sqlTable, rowsPerRead = sqlRowsPerRead, colInfo = ccColInfo)

rxGetInfo(nyc_sql, getVarInfo = TRUE, numRows = 3) # show column types and the first 10 rows

system.time(
  rxsum_sql <- rxSummary(~., nyc_sql) # provide statistical summaries for all the columns
)
rxsum_sql

ystem.time(linmod <- rxLinMod(tip_percent ~ pickup_nhood:dropoff_nhood + pickup_dow:pickup_hour,
                               data = nyc_sql, reportProgress = 0,
                               rowSelection = (u < .75)))

sqlTable <- "NYCTaxiScore"
nyc_score <- RxSqlServerData(connectionString = sqlConnString, rowsPerRead = sqlRowsPerRead,
                           table = sqlTable)

rxPredict(trained.models$linmod, data = nyc_sql, outData = nyc_score, predVarNames = "tip_percent_pred_linmod", overwrite = TRUE, rowSelection = (u >= .74))