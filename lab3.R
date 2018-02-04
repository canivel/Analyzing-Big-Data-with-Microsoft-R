library(RevoScaleR)

input_xdf <- 'mht_lab2.xdf'

mht_lab2 <- RxXdfData(input_xdf)

head(mht_lab2)

jfk_cash = rxCrossTabs(~Ratecode_type_desc:payment_type_desc, mht_lab2)

duration_card = rxCube(~ payment_type_desc, mht_lab2,
    rowSelection = (
             trip_distance > 5 &
               trip_duration <= 600 &
               payment_type_desc == 'card' 
               ))

#tip_range = rxCrossTabs(~fare_amount:tip_amount, mht_lab2)

#tip_range <- rxCube(range_tip ~ fare_amount:tip_amount, mht_lab2,
#               transforms = list(range_tip = (tip_amount * 100) / fare_amount))
xforms <- function(data) {
    data$tip_range <- (data$tip_amount * 100) / data$fare_amount
    data
}

rxDataStep(mht_lab2, mht_lab2, overwrite = TRUE, transformFunc = xforms)

rxSummary(~tip_percent, mht_lab2)

rxHistogram(~tip_percent, mht_lab2, rowSelection = (
    payment_type_desc == 'cash'))


rxHistogram(~tip_percent , mht_lab2, rowSelection = (
    payment_type_desc == 'card'))

xforms <- function(data) {
    data$tip_range <- ifelse(data$tip_percent <= 5, 0, data$pickup_longitude)
    data
}

rxHistogram(tip_percent ~ Ratecode_type_desc, mht_lab2, rowSelection = (
    payment_type_desc == 'card'), blocksPerRead = 5)

rxCube(tip_percent ~ Ratecode_type_desc, mht_lab2)
