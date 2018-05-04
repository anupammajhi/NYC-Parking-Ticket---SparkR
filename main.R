
# load SparkR
library(SparkR)
library(magrittr)
library(ggplot2)

# Initialize spark session
sparkR.session(master='local')

###################################################################################################################
########################################## Data Preparation and Cleaning ##########################################

# Reading the CSV files from S3 bucket
NYCParking_2015 <- SparkR::read.df("s3://nycparkinghari/Parking_Violations_Issued_-_Fiscal_Year_2015.csv",source="csv",header="true",inferSchema="true")
NYCParking_2016 <- SparkR::read.df("s3://nycparkinghari/Parking_Violations_Issued_-_Fiscal_Year_2016.csv",source="csv",header="true",inferSchema="true")
NYCParking_2017 <- SparkR::read.df("s3://nycparkinghari/Parking_Violations_Issued_-_Fiscal_Year_2017.csv",source="csv",header="true",inferSchema="true")

# Examining structure
str(NYCParking_2015) # 51 variables
str(NYCParking_2016) # 51 variables
str(NYCParking_2017) # 43 variables

#In 2017 Data Following headers are missing
# => Latitude
# => Longitude
# => Community Board
# => Community Council
# => Census Tract
# => BIN
# => BBL
# => NTA

# Number of rows each dataframe
nrow(NYCParking_2015) #11809233
nrow(NYCParking_2016) #10626899
nrow(NYCParking_2017) #10803028

# Looking for duplicate rows (only retaining distinct rows)
nrow(distinct(NYCParking_2015)) #10951257 - Hence there are duplicate rows
NYCParking_2015 <- distinct(NYCParking_2015) # Removing duplicates

nrow(distinct(NYCParking_2016)) #10626899 - No duplicate rows
nrow(distinct(NYCParking_2017)) #10803028 - No duplicate rows

# Sampling of each dataset and converting it to R Data Frames. This will help us analyse the data and understand what cleaning needs to be performed.

NYCParking_2015_sample <- sample(NYCParking_2015,withReplacement = F,fraction = 0.01)
NYCParking_2015_sample_R <- collect(NYCParking_2015_sample)

NYCParking_2016_sample <- sample(NYCParking_2016,withReplacement = F,fraction = 0.01) 
NYCParking_2016_sample_R <- collect(NYCParking_2016_sample)

NYCParking_2017_sample <- sample(NYCParking_2017,withReplacement = F,fraction = 0.01) 
NYCParking_2017_sample_R <- collect(NYCParking_2017_sample)

# Checking columns for number of missing values

sapply(NYCParking_2015_sample_R, function(x) sum(is.na(x)))
sapply(NYCParking_2016_sample_R, function(x) sum(is.na(x)))
sapply(NYCParking_2017_sample_R, function(x) sum(is.na(x)))

# We can see that columns from 'No Standing or Stopping Violation' are empty in all three datasets.
# Therefore, we need to remove them
# Hence we will only use columns 1 to 40

NYCParking_2015 <- NYCParking_2015[,1:40]
columns(NYCParking_2015)

NYCParking_2016 <- NYCParking_2016[,1:40]
columns(NYCParking_2016)

NYCParking_2017 <- NYCParking_2017[,1:40]
columns(NYCParking_2017)

# Adding ParsedIssue Date and a column for Fiscal Year to combine all 3 years data

NYCParking_2015 <- NYCParking_2015 %>% withColumn("Issue Date Parsed", to_date(NYCParking_2015$`Issue Date`,  "MM/dd/yyyy")) %>% 
  withColumn("Fiscal Year", "2015")

str(NYCParking_2015)

NYCParking_2016 <- NYCParking_2016 %>% withColumn("Issue Date Parsed", to_date(NYCParking_2016$`Issue Date`,  "MM/dd/yyyy")) %>% 
  withColumn("Fiscal Year", "2016")

str(NYCParking_2016)

NYCParking_2017 <- NYCParking_2017 %>% withColumn("Issue Date Parsed", to_date(NYCParking_2017$`Issue Date`,  "MM/dd/yyyy")) %>% 
  withColumn("Fiscal Year", "2017")

str(NYCParking_2017)

# creating sql views and Checking if issue dates in each Fiscal Year actually belong to the Fiscal Year
createOrReplaceTempView(NYCParking_2015,"NYC_2015_View") 
SparkR::sql("SELECT YEAR(`Issue Date Parsed`) AS Year,MONTH(`Issue Date Parsed`) AS Month,count(*) FROM NYC_2015_View GROUP BY Year,Month ORDER BY Year,Month") %>% head(num = 200)

createOrReplaceTempView(NYCParking_2016,"NYC_2016_View") 
SparkR::sql("SELECT YEAR(`Issue Date Parsed`) AS Year,MONTH(`Issue Date Parsed`) AS Month,count(*) FROM NYC_2016_View GROUP BY Year,Month ORDER BY Year,Month") %>% head(num = 200)

createOrReplaceTempView(NYCParking_2017,"NYC_2017_View") 
SparkR::sql("SELECT YEAR(`Issue Date Parsed`) AS Year,MONTH(`Issue Date Parsed`) AS Month,count(*) FROM NYC_2017_View GROUP BY Year,Month ORDER BY Year,Month") %>% head(num = 200)

# We can see that there are lot of rows which should not belong to the Fiscal years. Hence we need to filter them out.

NYCParking_2015 <- SparkR::sql("SELECT * FROM NYC_2015_View WHERE (YEAR(`Issue Date Parsed`) = 2014 and MONTH(`Issue Date Parsed`) >= 7) or (YEAR(`Issue Date Parsed`) = 2015 and MONTH(`Issue Date Parsed`) <= 6)")
NYCParking_2016 <- SparkR::sql("SELECT * FROM NYC_2016_View WHERE (YEAR(`Issue Date Parsed`) = 2015 and MONTH(`Issue Date Parsed`) >= 7) or (YEAR(`Issue Date Parsed`) = 2016 and MONTH(`Issue Date Parsed`) <= 6)")
NYCParking_2017 <- SparkR::sql("SELECT * FROM NYC_2017_View WHERE (YEAR(`Issue Date Parsed`) = 2016 and MONTH(`Issue Date Parsed`) >= 7) or (YEAR(`Issue Date Parsed`) = 2017 and MONTH(`Issue Date Parsed`) <= 6)")

# Creating view for sql
createOrReplaceTempView(NYCParking_2015,"NYC_2015_View") 
createOrReplaceTempView(NYCParking_2016,"NYC_2016_View") 
createOrReplaceTempView(NYCParking_2017,"NYC_2017_View") 

# Combining all 3 Fiscal years data into single dataframe
NYCParking_All <- SparkR::rbind(NYCParking_2015,NYCParking_2016)
NYCParking_All <- SparkR::rbind(NYCParking_All,NYCParking_2017)

nrow(NYCParking_All)
str(NYCParking_All)

#Creating view for sql
createOrReplaceTempView(NYCParking_All,"NYC_All_View")


#######################################################################################################
########################################## Examine the data. ##########################################


########### 1. Find total number of tickets for each year.

Num_of_Tickets <- SparkR::sql("SELECT `Fiscal Year`,count(`Summons Number`) as count_SummonsNumber \
                              FROM NYC_All_View \
                              GROUP BY `Fiscal Year`
                              ORDER BY `Fiscal Year`") %>% collect()
Num_of_Tickets

#      Fiscal Year    count_SummonsNumber     
#        2015              10598036                       
#        2016              10396894                       
#        2017              10539563                       


########### 2. Find out how many unique states the cars which got parking tickets came from.

unique_states <- SparkR::sql("SELECT `Fiscal Year`,count(distinct(`Registration State`)) as Count_State \
                             FROM NYC_All_View \
                             GROUP BY `Fiscal Year` \
                             ORDER BY `Fiscal Year` ") %>% collect()
unique_states

#   Fiscal Year                   Count_State
#     2015                            69
#     2016                            68
#     2017                            67

########### 3. Some parking tickets don’t have addresses on them, which is cause for concern. Find out how many such tickets there are.

empty_address <- SparkR::sql("SELECT `Fiscal Year`,count(`Fiscal Year`) as Frequency_InvalidAddress \
                             FROM NYC_All_View \
                             WHERE `House Number` IS NULL \
                             AND `Street Name` IS NULL \
                             AND `Intersecting Street` IS NULL \
                             GROUP BY `Fiscal Year` \
                             ORDER BY `Fiscal Year` ") %>% collect()

empty_address

#     Fiscal Year   Frequency_InvalidAddress
#        2015               3696
#        2016               2640
#        2017               2418 

#Assuming that Intersecting Street is not part of Address.
empty_address1 <- SparkR::sql("SELECT `Fiscal Year`,count(`Fiscal Year`) as Frequency_InvalidAddress \
                             FROM NYC_All_View \
                             WHERE `House Number` IS NULL \
                             AND `Street Name` IS NULL \
                             GROUP BY `Fiscal Year` \
                             ORDER BY `Fiscal Year` ") %>% collect()

empty_address1

#     Fiscal Year   Frequency_InvalidAddress
#        2015               3759
#        2016               2788
#        2017               2553 

#######################################################################################################
########################################## Aggregation tasks ##########################################

########### 1. How often does each violation code occur? (frequency of violation codes - find the top 5)

NYC_All_Violation_Grouped <- SparkR::sql("SELECT `Fiscal Year`,`Violation Code`,count(`Violation Code`) AS Violation_Frequency \ 
                                         FROM NYC_All_View \
                                         GROUP BY `Fiscal Year`,`Violation Code`")

createOrReplaceTempView(NYC_All_Violation_Grouped,"NYC_All_Violation_Grouped_View")

NYC_All_Violation_top5_peryear <- SparkR::sql("SELECT `Fiscal Year`,`Violation Code`, Violation_Frequency \
                                              FROM ( SELECT `Fiscal Year`,`Violation Code`, Violation_Frequency, \
                                              dense_rank() OVER(PARTITION BY `Fiscal Year` ORDER BY Violation_Frequency DESC) AS rank \
                                              FROM NYC_All_Violation_Grouped_View) \
                                              WHERE rank <= 5") %>% collect()

NYC_All_Violation_top5_peryear

    # Fiscal Year    Violation Code    Violation_Frequency
#         2016             21             1497269
#         2016             36             1232952
#         2016             38             1126835
#         2016             14              860045
#         2016             37              677805
#         2017             21             1500396
#         2017             36             1345237
#         2017             38             1050418
#         2017             14              880152
#         2017             20              609231
#         2015             21             1469228
#         2015             38             1305007
#         2015             14              908418
#         2015             36              747098
#         2015             37              735600

# Plot

NYC_All_Body_top5_peryear %>% ggplot(aes(as.character(`Vehicle Body Type`),Frequency)) +
  geom_bar(aes(fill=as.character(`Vehicle Body Type`)),stat="identity") + 
  facet_grid(.~`Fiscal Year`) +
  labs(x="Vehicle Body Type", fill="Vehicle Body Type",title="Frequency of Vehicle Body Type getting parking tickets")


###########  2. How often does each vehicle body type get a parking ticket? How about the vehicle make? (find the top 5 for both)

# For Vehicle Body Type

NYC_All_Body_Grouped <- SparkR::sql("SELECT `Fiscal Year`,`Vehicle Body Type`,count(`Vehicle Body Type`) AS Frequency \ 
                                    FROM NYC_All_View \
                                    GROUP BY `Fiscal Year`,`Vehicle Body Type`")

createOrReplaceTempView(NYC_All_Body_Grouped,"NYC_All_Body_Grouped_View")

NYC_All_Body_top5_peryear <- SparkR::sql("SELECT `Fiscal Year`,`Vehicle Body Type`, Frequency \
                                         FROM ( SELECT `Fiscal Year`,`Vehicle Body Type`, Frequency, \
                                         dense_rank() OVER(PARTITION BY `Fiscal Year` ORDER BY Frequency DESC) AS rank \
                                         FROM NYC_All_Body_Grouped_View) \
                                         WHERE rank <= 5") %>% collect()

NYC_All_Body_top5_peryear

#   Fiscal Year Vehicle Body Type Frequency
#         2016              SUBN   3393838
#         2016              4DSD   2936729
#         2016               VAN   1489924
#         2016              DELV    738747
#         2016               SDN    401750
#         2017              SUBN   3632003
#         2017              4DSD   3017372
#         2017               VAN   1384121
#         2017              DELV    672123
#         2017               SDN    414984
#         2015              SUBN   3341110
#         2015              4DSD   3001810
#         2015               VAN   1570227
#         2015              DELV    822041
#         2015               SDN    428571

# Plot

NYC_All_Body_top5_peryear %>% ggplot(aes(as.character(`Vehicle Body Type`),Frequency)) +
  geom_bar(aes(fill=as.character(`Vehicle Body Type`)),stat="identity") + 
  facet_grid(.~`Fiscal Year`) +
  labs(x="Vehicle Body Type", fill="Vehicle Body Type",title="Frequency of Vehicle Body Type getting parking tickets")

# For Vehicle Make

NYC_All_Make_Grouped <- SparkR::sql("SELECT `Fiscal Year`,`Vehicle Make`,count(`Vehicle Make`) AS Frequency \ 
                                    FROM NYC_All_View \
                                    GROUP BY `Fiscal Year`,`Vehicle Make`")

createOrReplaceTempView(NYC_All_Make_Grouped ,"NYC_All_Make_Grouped_View")

NYC_All_Make_top5_peryear <- SparkR::sql("SELECT `Fiscal Year`,`Vehicle Make`, Frequency \
                                         FROM ( SELECT `Fiscal Year`,`Vehicle Make`, Frequency, \
                                         dense_rank() OVER(PARTITION BY `Fiscal Year` ORDER BY Frequency DESC) AS rank \
                                         FROM NYC_All_Make_Grouped_View) \
                                         WHERE rank <= 5") %>% collect()

NYC_All_Make_top5_peryear

#   Fiscal Year Vehicle Make Frequency
#         2016         FORD   1297363
#         2016        TOYOT   1128909
#         2016        HONDA    991735
#         2016        NISSA    815963
#         2016        CHEVR    743416
#         2017         FORD   1250777
#         2017        TOYOT   1179265
#         2017        HONDA   1052006
#         2017        NISSA    895225
#         2017        CHEVR    698024
#         2015         FORD   1373157
#         2015        TOYOT   1082206
#         2015        HONDA    982130
#         2015        CHEVR    811659
#         2015        NISSA    805572

# Plot

NYC_All_Make_top5_peryear %>% ggplot(aes(as.character(`Vehicle Make`),Frequency)) +
  geom_bar(aes(fill=as.character(`Vehicle Make`)),stat="identity") + 
  facet_grid(.~`Fiscal Year`) +
  labs(x="Vehicle Make", fill="Vehicle Make",title="Frequency of Vehicle Make getting parking tickets")


###########  3. A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of:
###########  3a. Violating Precincts (this is the precinct of the zone where the violation occurred)

NYC_All_Viol_Precinct_Grouped <- SparkR::sql("SELECT `Fiscal Year`,`Violation Precinct`,count(`Violation Precinct`) AS Frequency \ 
                                             FROM NYC_All_View \
                                             GROUP BY `Fiscal Year`,`Violation Precinct`")

createOrReplaceTempView(NYC_All_Viol_Precinct_Grouped ,"NYC_All_Viol_Precinct_Grouped_View")

NYC_All_Viol_Precinct_top5_peryear <- SparkR::sql("SELECT `Fiscal Year`,`Violation Precinct`, Frequency \
                                                  FROM ( SELECT `Fiscal Year`,`Violation Precinct`, Frequency, \
                                                  dense_rank() OVER(PARTITION BY `Fiscal Year` ORDER BY Frequency DESC) AS rank \
                                                  FROM NYC_All_Viol_Precinct_Grouped_View) \
                                                  WHERE rank <= 5") %>% collect()

NYC_All_Viol_Precinct_top5_peryear 
