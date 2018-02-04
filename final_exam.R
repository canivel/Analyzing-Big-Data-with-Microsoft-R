#by Tuba ~ Danilo Canivel
setwd('F:/MicrosoftR/edx')
#GROUP1Q04
hist(subset(ChickWeight, Time == 21)$weight, freq = TRUE, breaks = 10)
rxHistogram(~weight, ChickWeight, rowSelection = (Time == 21), numBreaks = 10)

#GROUP1Q03
col_classes <- c('VendorID' = "factor",
                 'tpep_pickup_datetime' = "character",
                 'tpep_dropoff_datetime' = "character",
                 'passenger_count' = "integer",
                 'trip_distance' = "numeric",
                 'pickup_longitude' = "numeric",
                 'pickup_latitude' = "numeric",
                 'RateCodeID' = "factor",
                 'store_and_fwd_flag' = "factor",
                 'dropoff_longitude' = "numeric",
                 'dropoff_latitude' = "numeric",
                 'payment_type' = "factor",
                 'fare_amount' = "numeric",
                 'extra' = "numeric",
                 'mta_tax' = "numeric",
                 'tip_amount' = "numeric",
                 'tolls_amount' = "numeric",
                 'improvement_surcharge' = "numeric",
                 'total_amount' = "numeric",
                 'u' = "numeric")
input_csv <- 'yellow_tripsample_2016-01.csv'
data_df <- rxImport(input_csv, colClasses = col_classes)
class(data_df) # "[1] "data.frame""

#GROUP1Q05
plot(cars)
rxLinePlot(dist ~ speed, cars, type = "p")

#GROUP1Q01
input_xdf <- 'yellow_tripdata_2016.xdf'
data_df_2 <- rxImport(input_csv, input_xdf, colClasses = col_classes, overwrite = TRUE)
head(data_df_2)

#GROUP1Q02
data_csv <- RxTextData(input_csv, colClasses = col_classes)
rxSummary(~total_amount, data_csv)
rxSummary(~total_amount, input_csv)
rxSummary(~total_amount, 'yellow_tripsample_2016-01.csv')

#GROUP2Q10
airquality
input_air <- 'airquality.xdf'
air_xdf = rxImport(airquality, input_air, overwrite = TRUE)

rxCube(Temp ~ factor(Day, levels = 1:31):factor(Month, levels = 1:12), air_xdf)

summary(air_xdf)

rxCrossTabs(Temp ~ factor(Day, levels = 1:31):factor(Month, levels = 1:12), input_air, means = TRUE)

summary(air_xdf)

#GROUP2Q07

hist(airquality$Temp, freq = TRUE, breaks = 10)
rxHistogram(~Temp, "airquality.xdf", numBreaks = 10)

#GROUP2Q11
rxGetInfo("airquality.xdf", getVarInfo = TRUE)
rxSummary()
head(airquality)
newlevs <- c("1973", "1974", "1975")
rxFactors("airquality.xdf", outFile = "airquality.xdf",
          factorInfo = list(Year = list(newLevels = unique(newlevs))))

rxDataStep("airquality.xdf", outFile = "airquality.xdf",
           transforms = list(Year = factor(Year, levels = newlevels)),
           transformObjects = list(newlevels = unique(newlevs)),
           overwrite = TRUE)

#GROUP2Q16
z <- airquality[, c("Wind", "Temp")]
cl <- kmeans(z, 2)
cl$cluster

cl2 <- rxKmeans(~Wind + Temp, data = z, numClusters = 2)
cl2$cluster

#GROUP2Q06
file_1 <- input_csv
file_2 <- input_xdf
rxImport(file_1, file_2, colClasses = col_classes, overwrite = TRUE)

rxGetInfo(file_2, getVarInfo = TRUE)
#tolls_amount will be used instead if total_amount > 10 high else Low

d1 <- rxDataStep(file_2, file_2,
           transforms = list(HiLo = ifelse(total_amount > 10, "High", "Low")),
           overwrite = TRUE)

d2 <- rxDataStep(file_1, file_2,
           transforms = list(HiLo = ifelse(total_amount > 10, "High", "Low")),
           overwrite = TRUE)

head(d1)
head(d2)

#GROUP2Q14

father.son <- read.table("http://stat-www.berkeley.edu/users/juliab/141C/pearson.dat", sep = " ")[, -1]

regfit <- lm(Temp ~ Wind, data = airquality)
regfit

regfit2 <- rxLinMod(Temp ~ Wind, data = "airquality.xdf")
regfit2

regfit3 <- rxLinMod(Temp ~ Wind, data = airquality)
regfit3


#GROUP2Q13
regfit_trip_percent <- rxLinMod(tip_percent ~ trip_distance:payment_type, data_df)

#GROUP2Q21
rxOptions(reportProgress = 0)

#GROUP2Q22
sqlConnString <- "Driver=SQL Server;Server=SETHMOTTDSVM;Database=RDB;Uid=ruser;Pwd=ruser"
sqlRowsPerRead <- 100000
sqlTable <- "NYCTaxiBig"
nyc_sql <- RxSqlServerData(connectionString = sqlConnString, rowsPerRead = sqlRowsPerRead, table = sqlTable)

sqlCC <- RxInSqlServer(connectionString = sqlConnString)

#GROUP2Q08

rxct <- rxCrossTabs(Temp ~ factor(Day, levels = 1:31):factor(Month, levels = 1:12), "airquality.xdf", means = TRUE)
head(rxct)

rxct <- rxCrossTabs(Temp ~ factor(Day, levels = 1:31):factor(Month, levels = 1:12), "airquality.xdf")
print(rxct, output = "means")

#GROUP2Q12
newlevs <- c("1973", "1974", "1975")
rxDataStep("airquality.xdf", outFile = "airquality.xdf",
               transforms = list(Year = factor(Year, levels = newlevels)),
               transformObjects = list(newlevels = unique(newlevs)),
               overwrite = TRUE)



rxFactors("airquality.xdf", outFile = "airquality.xdf",
          factorInfo = list(Year = list(newLevels = unique(newlevs))))

#GROUP2Q04

athreshold <- 10
file_1 <- input_csv
file_2 <- input_xdf
rxImport(file_1, file_2, colClasses = col_classes, overwrite = TRUE)
xforms <- function(data) {
    data$HiLo <- ifelse(data$total_amount > threshold, "High", "Low")
    data
}

d1 <- rxDataStep(file_2, file_2,
           transformFunc = xforms,
           transformObjects = list(threshold = athreshold),
           overwrite = TRUE)

head(d1)

#GROUP2Q09

over75 <- rxDataStep("airquality.xdf",
           rowSelection = (HiLo == "High"),
           transforms = list(HiLo = ifelse(Temp > 75, "High", "Low")))

head(over75)

#GROUP2Q17
cor(airquality)

rxCor(~Ozone + Solar.R + Wind + Temp + Month + Day, airquality)


#GROUP2Q01
head(data_df_2)
rxSummary(~tip_amount, data_df_2)

#GROUP2Q18
model1 <- rxLinMod(tip_amount ~ trip_distance, data = input_xdf)
model1

model2 <- rxDTree(tip_percent ~ trip_distance, data = input_xdf)
model2

#GROUP2Q15
