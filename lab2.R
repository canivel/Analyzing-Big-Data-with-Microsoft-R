library(RevoScaleR)
setwd('F:/MicrosoftR/edx')

input_xdf <- 'nyc_lab1.xdf'

nyc_xdf <- RxXdfData(input_xdf, colCla)

rxSummary(~payment_type, nyc_xdf)

rxGetInfo(nyc_xdf, getVarInfo = TRUE, numRows = 10)

rxDataStep(nyc_xdf, nyc_xdf, transformVars = c("payment_type"),
           transforms = list(payment_type_desc = ifelse(payment_type > 0 & payment_type < 5,
                                                   as.factor(payment_type), NA)),
           overwrite = TRUE)

rxDataStep(nyc_xdf, nyc_xdf, transformVars = c("RatecodeID"),
           transforms = list(Ratecode_type_desc = as.factor(RatecodeID)),
           overwrite = TRUE)


rxGetVarInfo(nyc_xdf)

xforms <- function(data) {
    # transformation function for extracting some date and time features
    payment_type_labels = c('Credit card', 'Cash', 'No charge', 'Dispute', 'Unknown', 'Voided trip')
    cut_payment_type_levels <- c(1, 2, 3, 4)

    rate_code_id_labels = c('Standard Rate', 'JFK', 'Newark', 'Nassau or Westchester', 'Negotiated fare', 'Group ride')
    cut_ratecode_levels <- c(1, 2, 3, 4, 5, 6, 99)

    payment_type <- addNA(cut(data$payment_type, cut_payment_type_levels))
    payment_type_transform <- factor(payment_type, levels = 1:4, labels = payment_type_labels)

    rate_codes <- addNA(cut(data$RatecodeID, cut_ratecode_levels))
    rate_codes_transform <- factor(rate_codes, levels = 1:7, labels = cut_ratecode_levels)

    data$payment_type_transform <- payment_type_transform
    data$rate_codes_transform <- rate_codes_transform

    data
}


rxSummary(~Ratecode_type_desc, nyc_xdf)

rxSummary(~payment_type_desc, nyc_xdf)