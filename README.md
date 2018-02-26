**Introduction**
I have been working as a data engineer for more than 2 years. Most of my data engineering work is on distributed systems like Spark, Hadoop, Casandra etc.

I am a spark developer and likes to code in Scala since it’s a native programming language for Apache Spark. Most of my code was written in a distributed way which Spark likes. I often tend to use map, flatmap and filter heavily since they are distributed functions called as transformations on Spark to leverage Spark better.

**Development Directory:**
In src directory there is GenerateRepeatDonorFile.scala file which basically contains all the code required to execute this test.

I have also written unit test cases using scale using Scala Funsuite package.

**Test Directory:**
I have added to two test scenarios 10k record file and also a more than 1 million record file. In my test directory currently there should be three test directories. Please do check.

**Run.sh:**
In this file I have added two command one for compilation and the other for executing the code. Please do not forget to increase the java heap size if you want to test my code on large datasets.

I have tried not to include any external libraries and dependencies. My code should work on simple Scala, this is pure Scala code with no external libraries etc. Since it is mentioned that we are not allowed to use any computation engine I have followed this approach.

**Conclusion:**
I have implemented this code using LinkedHashMap which preserves order and is very efficient. However, there are few other areas where I need to write code efficiently. For example, while reading a file and generate schema I need to find an efficient way to do that. Because of very less time and my busy office with two different projects I am giving my best here.

I really enjoyed this coding challenge. Thanks for Insight data science for giving me this opportunity.

**Improvements to the codebase:**
I have improved my read functions, instead of loading all the 21 columns, now it only loads 6 columns. This should improve the performace of the reads.

I have also add 6 unit test cases using the scalatest framework FunSuite. FunSuite is very similar ot Junit and I have been FunSuite since an year now, I have pushed code on to the stage/prod using FunSuite.

I have also written unittest cases on cucumber, FlatSpec etc.

My code had issues while generating percentile in last commit which I didn't notice. I have updated the changes now it's should work.

I will try to learn and improve as much as I can.
