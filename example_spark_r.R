#files

data_path <- file.path("/user/RevoShare/blablabla")
taxi_path <- file.path(data_path, "taxidata")

rxHadoopListFiles(taxi_path)
taxi_xdf <- file.path(data_path, 'xdftaxidata')

#sparkhdfs

myNameNode <- "default"
myPort <- 0
hdfsFs <- RxHdfsFileSystem(hostName = myNameNode, port = myPort)


taxi_text = RxTextData(taxi_path, fileSystem = hdfsFs)
taxi_xdf = RxXdfData(taxi_xdf, fileSystem = hdfsFs)

spark_cc = RxSpark(consoleOutput = TRUE,
                   nameNode = myNameNode,
                   port = myPort,
                   executorCores = 13,
                   executorMem = "20g",
                   executorOverheadMen = "20g",
                   persistenRun = FALSE,
                   extraSparkConfig = "-- conf spark.speculation=true")

rxSetComputeContext(spark_cc)

#import data
system.time(rxImport(taxi_text, outFile = taxi_xdf))

#show data
rxGetInfo(taxi_xdf, getVarInfo = TRUE, numRows = 5)

#summarize
system.time(
    rxsum_xdf <- rxSummary( ~ fare_amount, taxi_xdf)
)

#some use cases
taxi_tip <- RxXdfData("/user/RevoShare/blablabla/xdftaxitips", fileSystem = hdfsFs)

system.time(
    rxDataStep(taxi_xdf, taxi_tip, transforms = list(tip_percent = ifelse(fare_amount > 0, tip_amount / fare_amount, NA)))
)

rxHistogram(~tip_percent, data = taxi_tip)

#distribution of tips
rxHistogram(~tip_percent,
            data = taxi_tip,
            rowSelection = (tip_percent < 5) & (tip_percent > 0),
            startVal = 0, endVal = 1, numBreaks = 21)


rxCrossTabs(~month:year, taxi_tip,
            transforms = list(
                    year = as.integer(substr(tpep_pickup_datetime, 1, 4)),
                    month = as.integer(substr(tpep_pickup_datetime, 6, 7)),
                    year = factor(year, levels = 2015:2016),
                    month = factor(month, levels = 1:12)))

#executing models in parallel

estimate_model <- function(xdf_data = freddie[["train"]], form = make_form(xdf_data, depVar = "default_flag"), model = rxLogin, ...) {
    rx_model <- model(form, data = xdf_data, ...)
    return (rx_model)
}

computeContext = RxSpark(consoleOutput = TRUE,
                           nameNode = myNameNode,
                           port = myPort,
                           executorCores = 6,
                           executorMem = "10g",
                           executorOverheadMen = "5g",
                           persistenRun = TRUE)

rxSetComputeContext(computeContext)

models <- list(rxLogit, rxDTree, rxDForest, rxBTrees)
trained_models <- rxExec(estimate_model, model = rxElemArg(models))

#predictions

default_pred <- function(model = default_model_gbm) {
    scored_xdf <- rxPredict(default_model_gbm, mort_split$validate, "scored.xdf")
    return(scored_xdf)
}

