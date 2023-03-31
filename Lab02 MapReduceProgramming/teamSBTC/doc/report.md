---
title: "Lab 02 : MapReduce Programming"
author: ["SBTC"]
date: "2023-4-1"
subtitle: "CSC14118 Introduction to Big Data 20KHMT1"
lang: "en"
titlepage: true
titlepage-color: "0B1887"
titlepage-text-color: "FFFFFF"
titlepage-rule-color: "FFFFFF"
titlepage-rule-height: 2
book: true
classoption: oneside
code-block-font-size: \scriptsize
---
# Lab 02 : MapReduce Programming


## Did you code by yourself or reference the solution?




## Explain the code in detail.
### Problem 2.1: Wordcount Program
Explain:
### Problem 2.2: WordSizeWordCount Program
Explain:
### Problem 2.3: WeatherData Program
![](images/2.3.1_Explain.png)

![](images/2.3.2_Explain.png)


Explain:

This code implements a Hadoop MapReduce job to process weather data and identify days with hot and cold temperatures. The input data contains multiple lines, each representing a single day's weather information. The mapper extracts the year, month, and day from each line, as well as the maximum and minimum temperatures. It then checks if the maximum temperature is greater than 40 or the minimum temperature is less than 10, and outputs a key-value pair with the date as the key and either "Hot Day" or "Cold Day" as the value, depending on which condition is satisfied.

The reducer simply collects all the key-value pairs with the same date and outputs them unchanged.
### Problem 2.4: Patent Program
Explain:
### Problem 2.5: MaxTemp Program
![](images/2.5.1_Explain.png)

![](images/2.5.2_Explain.png)

Explain:

The Map function is defined as a static inner class called Map, which extends the Mapper class. The Map class overrides the map method, which is called once for each line of the input file. In the map method, the input line is converted to a string, split into parts using a space as the delimiter, and the first part is saved as the key (the date) and the second part is saved as the value (the temperature). The key-value pair is emitted using the context object.

The Reduce function is defined as a static inner class called Reduce, which extends the Reducer class. The Reduce class overrides the reduce method, which is called once for each unique key (date) emitted by the Map function. In the reduce method, the values for a given key are received as an iterable object, which is traversed to find the maximum temperature. The maximum temperature is then emitted as a key-value pair using the context object.


### Problem 2.6: AverageSalary Program
Explain:
### Problem 2.7: De Identify HealthCare Program
![](images/2.7.1_Explain.png)

![](images/2.7.2_Explain.png)

Explain:
The DeIdentify class includes a Map class that implements the Mapper interface to read input data in a text format, encrypts the specific columns in the data table, and writes the modified data to a Hadoop cluster. The program also includes a helper function, encrypt(), which is used by the Map class to encrypt specific columns in the data table.

The program initializes a logger object to start logging data, specifies the parameters required to encrypt the data table's columns, and initializes the encryption key for the data table. The program uses the Cipher class to perform AES encryption and the Base64 class to encode the encrypted data into a string format.
### Problem 2.8: Music Track Program

Explain:
### Problem 2.9: Telecom Call Data Record Program
![](images/2.9.1_Explain.png)


![](images/2.9.2_Explain.png)

Explain:

In the "Map" class, the map method receives the CDR as an input, splits it into parts using the "|" delimiter, and checks if the call is a CDR by checking the value in the fifth position. If it is a CDR, the duration of the call is calculated by converting the start and end times to minutes using the "toMinutes" function and subtracting them. The phone number and call duration are then emitted as key-value pairs.

The "toMinutes" function converts a date string in the format "yyyy-MM-dd HH:mm:ss" to the time in minutes.

In the "Reduce" class, the reduce method receives the key-value pairs emitted by the Map class, which are grouped by phone number. The values are iterated over, and the duration of each call is summed up to get the total duration of calls for each phone number. The phone number and total call duration are then emitted as key-value pairs.

### Problem 2.10: Count Connected Component Program


Explain:

## Take screenshots of the running process and results.

### Problem 2.1: Wordcount Program

![](images/2.1.1.png)

![](images/2.1.2.png)

![](images/2.1.3.png)

![](images/2.1.4.png)

![](images/2.1.5.png)

### Problem 2.2: WordSizeWordCount Program

![](images/2.2.1.png)

![](images/2.2.2.png)

![](images/2.2.3.png)

![](images/2.2.4.png)

### Problem 2.3: WeatherData Program

![](images/2.3.1.png)

![](images/2.3.2.png)

![](images/2.3.3.png)

![](images/2.3.4.png)

### Problem 2.4: Patent Program

![](images/2.4.1.png)

![](images/2.4.2.png)

![](images/2.4.3.png)

![](images/2.4.4.png)

### Problem 2.5: MaxTemp Program

![](images/2.5.1.png)

![](images/2.5.2.png)

![](images/2.5.3.png)

![](images/2.5.4.png)

### Problem 2.6: AverageSalary Program

![](images/2.6.1.png)

![](images/2.6.2.png)

![](images/2.6.3.png)

![](images/2.6.4.png)

### Problem 2.7: De Identify HealthCare Program

![](images/2.7.1.png)

![](images/2.7.2.png)

![](images/2.7.3.png)

![](images/2.7.4.png)

### Problem 2.8: Music Track Program

![](images/2.8.1.png)

![](images/2.8.2.png)

![](images/2.8.3.png)

![](images/2.8.4.png)

### Problem 2.9: Telecom Call Data Record Program

![](images/2.9.1.png)

![](images/2.9.2.png)

![](images/2.9.3.png)

![](images/2.9.4.png)

### Problem 2.10: Count Connected Component Program

![](images/2.10.1.png)

![](images/2.10.2.png)

![](images/2.10.3.png)

![](images/2.10.4.png)

## Self-reflection.

### How much work, in percent (%),have you finished in each section?

  **My team works in section**

      |  Section 1 |  Section 2 | Section 3  |  Section 4 | Section 5  |
      |------------|------------|------------|------------|------------|
      |     100%   |     100%   |    100%    |    100%    |   100%     |

      |  Section 6 |  Section 7 | Section 8  |  Section 9 | Section 10 |
      |------------|------------|------------|------------|------------|
      |     100%   |     100%   |    100%    |    100%    |   100%     |

  **Strengths and weaknesses of my team**

  In general, some strengths could include things like being curious, motivated, persistent, organized, and having good teammate skills.

  Weaknesses could include things like having some problems which has much time to solve. First, we feel shy in asking for help from my team when needed afer that got to know each other better and help each other complete the Lab.

  

## References to your work
<!-- References without citing, this will be display as resources -->
[1] Sriram Balasubramanian. Hadoop-MapReduce Lab. University of California, Berkeley, 2016.

[2] Jimmy Lin and Michael Schatz. Design patterns for efficient graph algorithms in MapReduce.
In Proceedings of the Eighth Workshop on Mining and Learning with Graphs, pages 78–85,
Washington, D.C., July 2010. ACM

