
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

#  Fiscal Year Violation Precinct Frequency
#         2016                  0   1807139
#         2016                 19    545669
#         2016                 18    325559
#         2016                 14    318193
#         2016                  1    299074
#         2017                  0   1950083
#         2017                 19    528317
#         2017                 14    347736
#         2017                  1    326961
#         2017                 18    302008
#         2015                  0   1455166
#         2015                 19    550797
#         2015                 18    393802
#         2015                 14    377750
#         2015                  1    302737

# Plot

NYC_All_Viol_Precinct_top5_peryear %>% ggplot(aes(as.character(`Violation Precinct`),Frequency)) +
  geom_bar(aes(fill=as.character(`Violation Precinct`)),stat="identity") + 
  facet_grid(.~`Fiscal Year`) +
  labs(x="Violation Precinct", fill="Violation Precinct",title="Frequency of Violation Precinct getting parking tickets")
  

###########  3b. Issuing Precincts (this is the precinct that issued the ticket)

NYC_All_Issue_Precinct_Grouped <- SparkR::sql("SELECT `Fiscal Year`,`Issuer Precinct`,count(`Issuer Precinct`) AS Frequency \ 
                                              FROM NYC_All_View \
                                              GROUP BY `Fiscal Year`,`Issuer Precinct`")

createOrReplaceTempView(NYC_All_Issue_Precinct_Grouped ,"NYC_All_Issue_Precinct_Grouped_View")

NYC_All_Issue_Precinct_top5_peryear <- SparkR::sql("SELECT `Fiscal Year`,`Issuer Precinct`, Frequency \
                                                   FROM ( SELECT `Fiscal Year`,`Issuer Precinct`, Frequency, \
                                                   dense_rank() OVER(PARTITION BY `Fiscal Year` ORDER BY Frequency DESC) AS rank \
                                                   FROM NYC_All_Issue_Precinct_Grouped_View) \
                                                   WHERE rank <= 5") %>% collect()

NYC_All_Issue_Precinct_top5_peryear

#  Fiscal Year Issuer Precinct Frequency
#         2016               0   2067219
#         2016              19    532298
#         2016              18    317451
#         2016              14    309727
#         2016               1    290472
#         2017               0   2255086
#         2017              19    514786
#         2017              14    340862
#         2017               1    316776
#         2017              18    292237
#         2015               0   1648671
#         2015              19    536627
#         2015              18    384863
#         2015              14    363734
#         2015               1    293942

# Plot

NYC_All_Issue_Precinct_top5_peryear %>% ggplot(aes(as.character(`Issuer Precinct`),Frequency)) +
  geom_bar(aes(fill=as.character(`Issuer Precinct`)),stat="identity") + 
  facet_grid(.~`Fiscal Year`) +
  labs(x="Issuer Precinct", fill="Issuer Precinct",title="Frequency of Issuer Precinct getting parking tickets")
  
###########  4. Find the violation code frequency across 3 precincts which have issued the most number of tickets - do these precinct zones have an exceptionally high frequency of certain violation codes? Are these codes common across precincts?

# Top 3 precincts which issued maximum tickets

SparkR::sql("SELECT `Issuer Precinct`,count(`Issuer Precinct`) AS Frequency \ 
            FROM NYC_All_View \
            GROUP BY `Issuer Precinct` \
            ORDER BY Frequency") %>% head(num=3)

# 0,19,14 are the top 3 precincts which issued most tickets			

NYC_All_Violation_Per_Precinct_Grouped <- SparkR::sql("SELECT `Fiscal Year`,`Issuer Precinct`,`Violation Code`,count(`Violation Code`) AS Frequency \ 
                                                      FROM NYC_All_View WHERE `Issuer Precinct` IN (0,19,14) \
                                                      GROUP BY `Fiscal Year`,`Violation Code`,`Issuer Precinct`")

createOrReplaceTempView(NYC_All_Violation_Per_Precinct_Grouped ,"NYC_All_Violation_Per_Precinct_Grouped_View")

NYC_All_Violation_Per_Precinct_top5_peryear <- SparkR::sql("SELECT `Fiscal Year`,`Issuer Precinct`,`Violation Code`, Frequency \
                                                           FROM ( SELECT `Fiscal Year`,`Issuer Precinct`,`Violation Code`, Frequency, \
                                                           dense_rank() OVER(PARTITION BY `Fiscal Year`,`Issuer Precinct` ORDER BY Frequency DESC) AS rank \
                                                           FROM NYC_All_Violation_Per_Precinct_Grouped_View \
                                                           ) \
                                                           WHERE rank <= 5") %>% head(num=70)

NYC_All_Violation_Per_Precinct_top5_peryear

# Fiscal Year Issuer Precinct Violation Code Frequency
# 1         2017              14             14     73007
# 2         2017              14             69     57316
# 3         2017              14             31     39430
# 4         2017              14             47     30200
# 5         2017              14             42     20402
# 6         2016              14             69     66874
# 7         2016              14             14     61358
# 8         2016              14             31     35169
# 9         2016              14             47     23985
# 10        2016              14             42     23293
# 11        2015              14             69     79330
# 12        2015              14             14     75985
# 13        2015              14             31     40410
# 14        2015              14             42     27755
# 15        2015              14             47     26811
# 16        2015               0             36    747098
# 17        2015               0              7    567951
# 18        2015               0             21    173191
# 19        2015               0              5    127153
# 20        2015               0             66      4703
# 21        2016              19             38     76178
# 22        2016              19             37     74758
# 23        2016              19             46     71509
# 24        2016              19             14     60856
# 25        2016              19             21     57601
# 26        2017               0             36   1345237
# 27        2017               0              7    464690
# 28        2017               0             21    258771
# 29        2017               0              5    130963
# 30        2017               0             66      9281
# 31        2015              19             38     89102
# 32        2015              19             37     78716
# 33        2015              19             14     59915
# 34        2015              19             16     55762
# 35        2015              19             21     55296
# 36        2017              19             46     84789
# 37        2017              19             38     71631
# 38        2017              19             37     71592
# 39        2017              19             14     56873
# 40        2017              19             21     54033
# 41        2016               0             36   1232951
# 42        2016               0              7    457871
# 43        2016               0             21    226687
# 44        2016               0              5    106617
# 45        2016               0             66      7275

#Summary of table output above:

# Year    Precinct    Violation Code(descending order)
# 2017      0          36,07,21,05,66
#          14          14,69,31,47,42
#          19          46,38,37,14,21

# 2016      0          36,07,21,05,66
#          14          69,14,31,47,42
#          19          38,37,46,14,21

# 2015      0          36,07,21,05,66
#          14          69,14,31,42,47
#          19          38,37,14,16,21

# Plot

NYC_All_Violation_Per_Precinct_top5_peryear %>% ggplot(aes(as.character(`Violation Code`),Frequency)) +
  geom_bar(aes(fill=as.character(`Violation Code`)),stat="identity") + 
  facet_grid(`Issuer Precinct`~`Fiscal Year`) +
  labs(x="Violation Code", fill="Violation Code",title="Frequency of Violation Code getting parking tickets")

###########  5.You’d want to find out the properties of parking violations across different times of the day:
###########  5a. The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.
#       AND  5b. Find a way to deal with missing values, if any.

# If Violation Time is Null, we will replace that with "From Hours in Effect" column value
NYCParking_All_2 <- NYCParking_All
NYCParking_All_2$`Violation Time` <- ifelse(isNull(NYCParking_All_2$`Violation Time`) & NYCParking_All_2$`From Hours In Effect` != "ALL" ,NYCParking_All_2$`From Hours In Effect`,NYCParking_All_2$`Violation Time`)

# We start by splitting the string

NYCParking_All_2 <- NYCParking_All_2 %>% withColumn("Hour", substr(NYCParking_All_2$`Violation Time`,1,2)) %>% withColumn( "Minute", substr(NYCParking_All_2$`Violation Time`,4,5 ))%>% withColumn( "a_p", substr(NYCParking_All_2$`Violation Time`,6,6 ))
str(NYCParking_All_2)

# Next we concat the various strings
NYCParking_All_2 <- NYCParking_All_2 %>% withColumn("Violation Time String", concat_ws( ':' ,cast(NYCParking_All_2$`Hour`, 'string'), NYCParking_All_2$`Minute` ))
str(NYCParking_All_2)

# We then convert to unix_timestamp
NYCParking_All_2 <- NYCParking_All_2 %>% withColumn("Violation Time Parsed", unix_timestamp(NYCParking_All_2$`Violation Time String`, 'HH:mm'))
str(NYCParking_All_2)

# To get the correct hour, we use a condition to add 12 hours(12*60*60 in seconds)
NYCParking_All_2 <- NYCParking_All_2 %>% withColumn("Actual Time Parsed", 
                                                    ifelse(NYCParking_All_2$`a_p` == 'P', NYCParking_All_2$`Violation Time Parsed` + (12*60*60), NYCParking_All_2$`Violation Time Parsed`))
str(NYCParking_All_2)

# Finally we convert this to a timestamp format
NYCParking_All_2 <- NYCParking_All_2 %>% withColumn("Actual Violation Time", cast(NYCParking_All_2$`Actual Time Parsed`, 'timestamp'))
str(NYCParking_All_2)


# Checking for NA's

filter(NYCParking_All_2, isNull(NYCParking_All_2$`Actual Violation Time`)) %>% nrow()

# We have 2684 Missing Values. We will Remove these rows
NYCParking_All_2 <- filter(NYCParking_All_2, isNotNull(NYCParking_All_2$`Actual Violation Time`)) 


###########  5c. Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. For each of these groups, find the 3 most commonly occurring violations

# Splitting time into 6 bins


NYCParking_All_2 <- NYCParking_All_2 %>% withColumn("Time of Day", 
                                                    ifelse(hour(NYCParking_All_2$`Actual Violation Time`) >= 2 & hour(NYCParking_All_2$`Actual Violation Time`) < 6, 'Early Morning',
                                                           ifelse(hour(NYCParking_All_2$`Actual Violation Time`) >= 6 & hour(NYCParking_All_2$`Actual Violation Time`) < 10, 'Morning',
                                                                  ifelse(hour(NYCParking_All_2$`Actual Violation Time`) >= 10 & hour(NYCParking_All_2$`Actual Violation Time`) < 14, 'Afternoon',
                                                                         ifelse(hour(NYCParking_All_2$`Actual Violation Time`) >= 14 & hour(NYCParking_All_2$`Actual Violation Time`) < 18, 'Evening',
                                                                                ifelse(hour(NYCParking_All_2$`Actual Violation Time`) >= 18 & hour(NYCParking_All_2$`Actual Violation Time`) < 22, 'Night', 'Late Night'
                                                                                ))))))
str(NYCParking_All_2)


# Updating SQL View
createOrReplaceTempView(NYCParking_All_2, "NYC_All_View_2")


# Finding 3 most common violation across the time slots

topviol_across_time <- SparkR::sql("select `Fiscal Year` , `Time of Day` , `Violation Code`, count(*) as Frequency from NYC_All_View_2 group by `Fiscal Year`, `Time of Day`, `Violation Code` " )


createOrReplaceTempView(topviol_across_time, "topviol_across_time_view")

topviol_across_time_top3 <- SparkR::sql("SELECT `Fiscal Year`,`Time of Day`, `Violation Code`,  Frequency 
                                        FROM ( SELECT `Fiscal Year`,`Time of Day`, `Violation Code`,  Frequency, 
                                        dense_rank() OVER(PARTITION BY `Fiscal Year`, `Time of Day` ORDER BY Frequency DESC) AS rank 
                                        FROM topviol_across_time_view) 
                                        WHERE rank <= 3") %>% collect()
topviol_across_time_top3
										
# Obtained Output:
# No.Fiscal Year Time of Day    Violation Code Frequency
# 1         2017 Early Morning             40     50460
# 2         2017 Early Morning             21     32248
# 3         2017 Early Morning             14     26719
# 4         2017    Late Night             36    204242
# 5         2017    Late Night             21    186348
# 6         2017    Late Night             38    110884
# 7         2017       Evening             38    379239
# 8         2017       Evening             37    284663
# 9         2017       Evening             14    235729
# 10        2017       Morning             21    844285
# 11        2017       Morning             36    353910
# 12        2017       Morning             14    273567
# 13        2015         Night              7    101896
# 14        2015         Night             38    100776
# 15        2015         Night             14     49055
# 16        2015 Early Morning             40     40851
# 17        2015 Early Morning             14     26663
# 18        2015 Early Morning             21     24944
# 19        2017     Afternoon             36    594090
# 20        2017     Afternoon             21    436648
# 21        2017     Afternoon             38    330862
# 22        2016    Late Night             21    172421
# 23        2016    Late Night             36    164171
# 24        2016    Late Night             38    124295
# 25        2016       Evening             38    384862
# 26        2016       Evening             37    314841
# 27        2016       Evening             36    256624
# 28        2016 Early Morning             40     40224
# 29        2016 Early Morning             14     28674
# 30        2016 Early Morning             21     26697
# 31        2016         Night             38     86443
# 32        2016         Night              7     82456
# 33        2016         Night             14     46568
# 34        2015       Evening             38    445972
# 35        2015       Evening             37    340951
# 36        2015       Evening             14    237059
# 37        2015    Late Night             21    168867
# 38        2015    Late Night             38    141099
# 39        2015    Late Night             36    100871
# 40        2016     Afternoon             36    475441
# 41        2016     Afternoon             21    453488
# 42        2016     Afternoon             38    358220
# 43        2015       Morning             21    817539
# 44        2015       Morning             14    280973
# 45        2015       Morning             36    215373
# 46        2016       Morning             21    843694
# 47        2016       Morning             36    336379
# 48        2016       Morning             14    271737
# 49        2017         Night              7     84112
# 50        2017         Night             38     73387
# 51        2017         Night             14     48595
# 52        2015     Afternoon             21    456602
# 53        2015     Afternoon             38    411271
# 54        2015     Afternoon             36    289998									
										

#Summary of table above: 
#   Year    Time of Day    Most Occuring Violation Codes in Descending order

#   2017    Morning         21,36,14
#           Afternoon       36,21,38
#           Evening         38,37,14
#           Night           07,38,14
#           Late Night      36,21,38
#           Early Morn      40,21,14

#   2016    Morning         21,36,14
#           Afternoon       36,21,38
#           Evening         38,37,36
#           Night           38,07,14
#           Late Night      21,36,38
#           Early Morn      40,14,21

#   2015    Morning         21,14,36
#           Afternoon       21,38,36
#           Evening         38,37,14
#           Night           07,38,14
#           Late Night      21,38,36
#           Early Morn      40,14,21

# We can see that the Violation Codes are fairly similar across all years for various times of day

# Plot

topviol_across_time_top3 %>% ggplot(aes(as.character(`Violation Code`),Frequency)) +
  geom_bar(aes(fill=as.character(`Violation Code`), alpha = 0.4),stat="identity") + 
  facet_grid(`Time of Day`~`Fiscal Year` ) +
  labs(x="Violation Code", fill="Violation Code", title="Frequency of Most Common Violation Across Time-Slots")

# The codes 14, 21 and 38 are common acrooss all the different time slots

###########  5d. Now, try another direction. For the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)

toptime_across_viol <- SparkR::sql("select `Fiscal Year`, `Violation Code` ,`Time of Day`, count(*) as Frequency from NYC_All_View_2 
                                   where `Violation Code` = 14 or `Violation Code` = 21 or `Violation Code` = 38 group by `Fiscal Year` , `Violation Code`, `Time of Day`  " )


createOrReplaceTempView(toptime_across_viol, "toptime_across_viol_view")

toptime_across_viol_top5 <- SparkR::sql("SELECT `Fiscal Year`, `Violation Code`, `Time of Day`,  Frequency 
                                        FROM ( SELECT `Fiscal Year`, `Violation Code`, `Time of Day`, Frequency, 
                                        dense_rank() OVER(PARTITION BY `Fiscal Year`, `Violation Code` ORDER BY Frequency DESC) AS rank 
                                        FROM toptime_across_viol_view) 
                                        WHERE rank <= 5") %>% collect()
toptime_across_viol_top5
										
#Obtained Output:
# No.   Fiscal Year Violation Code   Time of Day Frequency
# 1         2017             14       Morning    273567
# 2         2017             14       Evening    235729
# 3         2017             14     Afternoon    201580
# 4         2017             14    Late Night     93952
# 5         2017             14         Night     48595
# 6         2016             14       Morning    271737
# 7         2016             14       Evening    218313
# 8         2016             14     Afternoon    198623
# 9         2016             14    Late Night     96113
# 10        2016             14         Night     46568
# 11        2017             21       Morning    844285
# 12        2017             21     Afternoon    436648
# 13        2017             21    Late Night    186348
# 14        2017             21 Early Morning     32248
# 15        2017             21       Evening       548
# 16        2015             14       Morning    280973
# 17        2015             14       Evening    237059
# 18        2015             14     Afternoon    214853
# 19        2015             14    Late Night     99802
# 20        2015             14         Night     49055
# 21        2016             21       Morning    843694
# 22        2016             21     Afternoon    453488
# 23        2016             21    Late Night    172421
