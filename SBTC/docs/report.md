---
title: "Lab 01: A Gentle Introduction to Hadoop"
author: ["SBTC"]
date: "2023-02-17"
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
# Lab 01: A Gentle Introduction to Hadoop

## Setting up Single-node Hadoop Cluster
- Install java
  
![](images/1.2.0.png)
- Check java settings and path
  
![](images/1.2.1.png)
- Install openSSH
  
![](images/1.2.2.png)
- Create and Install SSH Certificates
  
![](images/1.2.3.png)
![](images/1.2.4.png)
![](images/1.2.5.png)
![](images/1.2.6.png)
-  Configure file core-site.xml
  
![](images/1.2.7.png)
- Configure the file mapred-site.xml
  
![](images/1.2.8.png)
- Configure file hdfs-site.xml
  
![](images/1.2.9.png)
- Configure file yarn-site.xml
  
![](images/1.2.10.png)
- Configure file hadoop-env.sh
  
![](images/1.2.11.png)
- Configure file bash
  
![](images/1.2.12.png)
- Successful installation
  
![](images/1.2.13.png)


### Cài thành công của thành viên trong nhóm:
- 20127444 - Bùi Duy Bảo
  
![](images/20127444.png)
- 20127049 - Nguyễn Đức Minh
  
![](images/20127049.png)
- 20127092 - Nguyễn Minh Tuấn
  
![](images/20127092.png)
- 20127448 - Nguyễn Thái Bảo
  
![](images/20127448.png)
   

## Introduction to MapReduce
1. **How do the input keys-values, the intermediate keys-values, and the output keys-values relate?**
  
The input keys-values are the initial data that is fed into the MapReduce job.

The intermediate keys-values are the pairs produced by the mapper nodes during the processing of the input data.

The output keys-values are the pairs produced by the reducer nodes after processing the intermediate pairs.

2. **How does MapReduce deal with node failures?**
   
MapReduce deals with node failure by being designed to be fault-tolerant and by using some techniques, for example data replication and job restart even node failure appears.

About by being designed to be fault-tolerant: it means that MapReduce can handle node failures and continue processing the job. If a node fails, the tasks running on that node are automatically reassigned to other available nodes. The framework also periodically pings the nodes to check if they are still alive. If a node does not respond, it is assumed to have failed, and its tasks are reassigned to other nodes.

About by using data replication: MapReduce uses this technique to ensure that data is not lost when node failure happens, each block of data is replicated across multiple nodes in the cluster. If one node fails, the data is still available on the other nodes, and the job can continue processing.

About by using job restart: in this situation, the entire job may need to be restarted. This is because if a node fails while processing a task, the output of that task may be lost. If the output of a task is lost, any subsequent tasks that depend on that output will need to be rerun.

3. **What is the meaning and implication of locality? What does it use?**
   
In mapreduce,locality means to processing data on the same computer or in same rack to minimize connection costs between machines and maximize data processing performance. 
Locality in MapReduce principle that it operates by shifting computation close to the location of the actual data on the node, as opposed to sending huge data to computation. This implies less traffic going through your systems, lower network stress, and a significantly more efficient use of limited bandwidth , in turn helping to decrease expenses, and boost overall network and system performance.

MapReduce uses locality in two ways : 

Data locality: This is the act of processing data on the same computer that stores the data. The MapReduce framework arranges for tasks to be carried out on the same machine as the input data.
    
Task locality: This refers to the processing of data on a device that is close to the device that stores the data. Tasks are scheduled on machines that are physically proximate to the machines where the data is kept in the MapReduce framework.

4. **Which problem is addressed by introducing a combiner function to the MapReduce model?**
   
In MapReduce process, Combiner is not a compulsory step after Map tasks and before Reduce tasks which optimizes the process to execute effectively. However, not every cases would be suitable to use Combiner. 

There are some situations, the sets of keys and values have to be run in mathematical stages which the results must be the same as that without Combiner. Only commutative and associative logic fits in Combiner phases, the more complex tasks would not be easy for Combiner to handle. 
   
For example: complicated approach method, string handling, data processing,… are ineligible in Combiners. Therefore, in some MapReduce platforms, Combiners are set as not compulsory phases because they not always handle all complex tasks

## Running a warm-up problem: Word Count
- Check Hadoop version
  
![](images/1.3.0.png)
- The input directory contains the file input.txt
  
![](images/1.3.1.png)
![](images/1.3.2.png)
- Enter the following path to export the Hadoop classpath to bash
- Make sure it's now exported

![](images/1.3.3.png)
- Create this directory on HDFS and put input.txt . file

![](images/1.3.4.png)
![](images/1.3.5.png)
![](images/1.3.6.png)
- Run file jar on Hadoop
  
![](images/1.3.7.png)
![](images/1.3.8.png)
![](images/1.3.9.png)
- Output 
  
![](images/1.3.10.png)
![](images/1.3.11.png)
![](images/1.3.12.png)

    
    
## Bonus
4.1 Extended Word Count: Unhealthy relationships
- Input 
  
![](images/1.4.0.png)
- Output 
  
![](images/1.4.1.png)
- Directory containing output on hadoop

![](images/1.4.2.png)

  
![](images/1.4.3.png)
- Visualize example
  
![](images/1.4.4.png)

## References
<!-- References without citing, this will be display as resources -->
- Two Cloudera version of WordCount problem:
    - https://docs.cloudera.com/documentation/other/tutorial/CDH5/topics/ht_wordcount2.html
    - https://docs.cloudera.com/documentation/other/tutorial/CDH5/topics/ht_wordcount3.html
- Book: MapReduce Design Patterns [Donald Miner, Adam Shook, 2012]
- All of StackOverflow link related.
- Another link 
  - https://community.cloudera.com/t5/Support-Questions/class-org-apache-hadoop-io-MapWritable-is-not-class-org/m-p/136602?fbclid=IwAR3VazsXyHo688u4UyNvsBdj_a_2hOwuJ-KpGZD6fg4aMwW8trQlpz-KArA
  - https://stackoverflow.com/questions/56153007/java-lang-exception-java-io-ioexception-wrong-value-class-while-setting-hadoop?fbclid=IwAR1UNjMqPVvXy9nMberqIHAYFNZ02q0we1BAoqZOGUhmEFArVmbnBfyoCiQ
  - https://www.thoughtworks.com/insights/decoder/d/data-locality?fbclid=IwAR1BwoHh0Q8-hylq51ylms9NIWQI300GMKdZm4yydbQkEDBWhrbcqQoM554
  - https://stackoverflow.com/questions/58272650/what-exactly-does-data-locality-mean-in-hadoop?fbclid=IwAR0jSqFAXazrxBUA7uO8sZsGWbhkry6SYJAf4PtBEbNfilKoZcl6HTBbdLk
  - https://www.tutorialspoint.com/map_reduce/map_reduce_quick_guide.html
- 
  - https://www.youtube.com/watch?v=6sK3LDY7Pp4&t=483s
  - https://www.youtube.com/watch?v=MZ_FUEnbrR4

<!-- References with citing, this will be display as footnotes -->
[^fn1]: So Chris Krycho, "Not Exactly a Millennium," chriskrycho.com, July 2015, http://v4.chriskrycho.com/2015/not-exactly-a-millennium.html
(accessed July 25, 2015)

[^fn2]: Contra Krycho, 15, who has everything _quite_ wrong.

[^fn3]: ibid