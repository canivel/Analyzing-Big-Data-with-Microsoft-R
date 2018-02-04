library(RevoScaleR)

setwd('F:/MicrosoftR/edx')

nyc_xdf <- RxXdfData('nyc_lab1.xdf')

rxGetInfo(nyc_xdf, getVarInfo = TRUE, numRows = 10)

rxSummary(~factor(RatecodeID, levels = 1:99) +
             factor(payment_type, levels = 1:4), nyc_xdf)