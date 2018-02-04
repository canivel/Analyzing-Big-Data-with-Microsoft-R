library(RevoScaleR)

input_xdf <- 'mht_lab2.xdf'

mht_lab2 <- RxXdfData(input_xdf)

form_1 <- as.formula(tip_percent ~ trip_duration + pickup_dow:pickup_hour)
rxlm_1 <- rxLinMod(form_1, data = mht_lab2, dropFirst = TRUE, covCoef = TRUE)

round(rxlm_1$adj.r.squared, 6)

form_2 <- as.formula(tip_percent ~ payment_type_desc + trip_duration + pickup_dow:pickup_hour)
rxlm_2 <- rxLinMod(form_2, data = mht_lab2, dropFirst = TRUE, covCoef = TRUE)

round(rxlm_2$adj.r.squared, 2)

rxs <- rxSummary(~trip_duration + pickup_dow + pickup_hour, mht_lab2)
#rxs2 <- rxSummary(~payment_type_desc + trip_duration + pickup_dow + pickup_hour, mht_lab2)


prediction_1 <- rxPredict(rxlm_1, mht_lab2, predVarNames = "tip_pred_1")
prediction_2 <- rxPredict(rxlm_2, mht_lab2, predVarNames = "tip_pred_2")

rxHistogram(~tip_percent, mht_lab2, startval = 0, endVal = 50)
rxHistogram(~tip_pred_1, mht_lab2)
rxHistogram(~tip_pred_2, mht_lab2)