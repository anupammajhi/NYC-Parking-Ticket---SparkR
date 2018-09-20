# Problem Statement

## Objectives of the Case Study

- Primarily, the case study is meant as a deep-dive into the usage of Spark. As you have seen while working with Spark, its syntax behaves differently from your regular R syntax. One of the major objectives of this case study is gaining familiarity with how an analysis works in SparkR as opposed to base R.
- Learning the basic idea behind using functions in SparkR can be transferred to using other libraries like PySpark. If you are in a company where Python is a primary language, you can easily pick up PySpark syntax and use Spark’s processing power.
- The actual process of running a model-building command, boils down to a few lines of code. In trying to find inference from data, the most time-consuming step is preparing the data upto the point of model-building. Hence, we’re gearing this case study more towards exploratory analysis.

 

## Problem Statement

Big data analytics allows you to analyse data at scale. It has applications in almost every industry in the world. Let’s consider an unconventional application that you wouldn’t ordinarily encounter.

 

New York City is a thriving metropolis. Just like most other metros that size, one of the biggest problems its citizens face, is parking. The classic combination of a **huge number of cars**, and a **cramped geography** is the exact recipe that leads to a huge number of parking tickets.

 

In an attempt to scientifically analyse this phenomenon, the NYC Police Department has **collected data for parking tickets**. Out of these, the data files from 2014 to 2017 are publicly available on Kaggle. We will try and perform some **exploratory analysis** on this data. Spark will allow us to**analyse the full files at high speeds**, as opposed to taking a series of random samples that will approximate the population.

 

For the scope of this analysis, we wish to compare phenomenon related to parking tickets over three different years - **2015, 2016, 2017**. All the analysis steps mentioned below should be done for 3 different years. Each metric you derive should be compared across the 3 years.

 

**Note**: although the broad goal of any analysis of this type would indeed be better parking and less tickets, we are **not looking for recommendations on how to reduce the number of parking tickets**- there are no points for this. We are instead looking for an **exploratory analysis** that helps us understand the data. The questions given below will guide your EDA.

 

The data dictionary is available [on this page](https://www.kaggle.com/new-york-city/nyc-parking-tickets/data) along with the data.

 

Here are the steps you need to perform.
 

**A> Uploading data into S3**

 

It is standard practice to use data from S3 while performing analysis in Spark. First, upload data into S3:

1. As a prerequisite, you will need to create a Kaggle account.
2. [Here is the link ](https://www.kaggle.com/new-york-city/nyc-parking-tickets/data)to the dataset in Kaggle.
3. There is a great StackOverflow answer on how to transfer files present on Kaggle, to an EC2 instance. [Here is the link](https://stackoverflow.com/questions/45261190/how-to-get-kaggle-competition-data-via-command-line-on-virtual-machine). Follow the instructions to copy the 3 files corresponding to 2015, 2016, 2017 to your Master node.
4. If you’re using a Windows machine:
   - You would have to connect to the Master node using SSH through PuTTY and PuTTYGen
   - You would have to perform the action of copying a file using WinSCP. Watch [this video](https://www.youtube.com/watch?v=nSX4GjnmGlU) for explicit instructions on how to copy from your local Windows machine to an AWS EC2 instance (which is a Linux machine). (Note: this process is different from connecting via SSH using PuTTY, which you will also need to do.)
5. Once the files are in your Master node, upload them one by one to S3. [Here are the instructions](https://learn.upgrad.com/v/course/58/session/11891/segment/58738), in case you need them.

 

**B> Questions to be answered in the analysis**

The following analysis should be performed on RStudio mounted on your AWS cluster, using the SparkR library. Remember, you should do this analysis for all the 3 years, and possibly compare metrics and insights across the years.

 

Examine the data.

1. Find total number of tickets for each year.
2. Find out how many unique states the cars which got parking tickets came from.
3. Some parking tickets don’t have addresses on them, which is cause for concern. Find out how many such tickets there are.

 

Aggregation tasks

1. How often does each violation code occur? (frequency of violation codes - find the top 5)
2. How often does each vehicle body type get a parking ticket? How about the vehicle make? (find the top 5 for both)
3. A precinct is a police station that has a certain zone of the city under its command. Find the (5 highest) frequencies of:
   1. Violating Precincts (this is the precinct of the zone where the violation occurred)
   2. Issuing Precincts (this is the precinct that issued the ticket)
4. Find the violation code frequency across 3 precincts which have issued the most number of tickets - do these precinct zones have an exceptionally high frequency of certain violation codes? Are these codes common across precincts?
5. You’d want to find out the properties of parking violations across different times of the day:
   - The Violation Time field is specified in a strange format. Find a way to make this into a time attribute that you can use to divide into groups.
   - Find a way to deal with missing values, if any.
   - Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. For each of these groups, find the 3 most commonly occurring violations
   - Now, try another direction. For the 3 most commonly occurring violation codes, find the most common times of day (in terms of the bins from the previous part)
6. Let’s try and find some seasonality in this data
   - First, divide the year into some number of seasons, and find frequencies of tickets for each season.
   - Then, find the 3 most common violations for each of these season
7. The fines collected from all the parking violation constitute a revenue source for the NYC police department. Let’s take an example of estimating that for the 3 most commonly occurring codes.
   - Find total occurrences of the 3 most common violation codes
   - Then, search the internet for NYC parking violation code fines. You will find a website (on the nyc.gov URL) that lists these fines. They’re divided into two categories, one for the highest-density locations of the city, the other for the rest of the city. For simplicity, take an average of the two.
   - Using this information, find the total amount collected for all of the fines. State the code which has the highest total collection.
   - What can you intuitively infer from these findings?

 

**C>** **General Guidelines:**

1. Your submission will consist of one file of code and one text file. In the text file, you should write some subjective observations you have made from this data.
2. If you make any specific assumptions related to these questions, be sure to state them.
3. Your AWS clusters are costly, and one person's cluster cannot handle the full analysis. Since this is a 4-people case study, be efficient in dividing work between the 4 people.
4. Plot histograms/charts wherever you feel necessary.