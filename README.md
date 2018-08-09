# README #

Sparklens is a profiling tool for Spark with built-in Spark Scheduler simulator. Its primary goal is to make it easy 
to understand the scalability limits of spark applications. It helps in understanding how efficiently is a given 
spark application using the compute resources provided to it. May be your application will run faster with more 
executors and may be it wont. Sparklens can answer this question by looking at a single run of your application. 

It helps you narrow down to few stages (or driver, or skew or lack of tasks) which are limiting your application 
from scaling out and provides contextual information about what could be going wrong with these stages. Primarily 
it helps you approach spark application tuning as a well defined method/process instead of something you learn by 
trial and error, saving both developer and compute time. 

### What does it reports? ###

* Estimated completion time and estimated cluster utilisation with different number of executors
 
 ```
 Executor count    31  ( 10%) estimated time 87m 29s and estimated cluster utilization 92.73%
 Executor count    62  ( 20%) estimated time 47m 03s and estimated cluster utilization 86.19%
 Executor count   155  ( 50%) estimated time 22m 51s and estimated cluster utilization 71.01%
 Executor count   248  ( 80%) estimated time 16m 43s and estimated cluster utilization 60.65%
 Executor count   310  (100%) estimated time 14m 49s and estimated cluster utilization 54.73%
```
Given a single run of a spark application, Sparklens can estimate how will your application perform 
given any arbitrary number of executors. This helps you understand the ROI on adding executors. 

* Job/Stage timeline which shows how the parallel stages were scheduled within a job. This makes it easy to visualise 
the DAG with stage dependencies at the job level. 

```
07:05:27:666 JOB 151 started : duration 01m 39s 
[    668 ||||||||||||||||||||||                                                          ]
[    669 |||||||||||||||||||||||||||                                                     ]
[    673                                                                                 ]
[    674                         ||||                                                    ]
[    675                            |||||||                                              ]
[    676                                   ||||||||||||||                                ]
[    677                         |||||||                                                 ]
[    678                                                 |                               ]
[    679                                                  |||||||||||||||||||||||||||||||]
```

*Lots of interesting per stage metrics like Input, Output, Shuffle Input and Shuffle Output per stage. **OneCoreComputeHours** 
available and used per stage to find out inefficient stages. 

```
Total tasks in all stages 189446
Per Stage  Utilization
Stage-ID   Wall    Task      Task     IO%    Input     Output    ----Shuffle-----    -WallClockTime-    --OneCoreComputeHours---   MaxTaskMem
          Clock%  Runtime%   Count                               Input  |  Output    Measured | Ideal   Available| Used%|Wasted%                                  
       0    0.00    0.00         2    0.0  254.5 KB    0.0 KB    0.0 KB    0.0 KB    00m 04s   00m 00s    05h 21m    0.0  100.0    0.0 KB 
       1    0.00    0.01        10    0.0  631.1 MB    0.0 KB    0.0 KB    0.0 KB    00m 07s   00m 00s    08h 18m    0.2   99.8    0.0 KB 
       2    0.00    0.40      1098    0.0    2.1 GB    0.0 KB    0.0 KB    5.7 GB    00m 14s   00m 00s    16h 25m    3.2   96.8    0.0 KB 
       3    0.00    0.09       200    0.0    0.0 KB    0.0 KB    5.7 GB    2.3 GB    00m 03s   00m 00s    04h 35m    2.6   97.4    0.0 KB 
       4    0.00    0.03       200    0.0    0.0 KB    0.0 KB    2.3 GB    0.0 KB    00m 01s   00m 00s    01h 13m    2.9   97.1    0.0 KB 
       7    0.00    0.03       200    0.0    0.0 KB    0.0 KB    2.3 GB    2.7 GB    00m 02s   00m 00s    02h 27m    1.7   98.3    0.0 KB 
       8    0.00    0.03        38    0.0    0.0 KB    0.0 KB    2.7 GB    2.7 GB    00m 05s   00m 00s    06h 20m    0.6   99.4    0.0 KB 
```

Internally, Sparklens has a concept of Analyzer which is a generic component for emitting interesting events. 
Following Analyzers are currently available:

1. AppTimelineAnalyzer
2. EfficiencyStatisticsAnalyzer
3. ExecutorTimelineAnalyzer
4. ExecutorWallclockAnalyzer
5. HostTimelineAnalyzer
6. JobOverlapAnalyzer
7. SimpleAppAnalyzer
8. StageOverlapAnalyzer
9. StageSkewAnalyzer

We are hoping that spark experts world over will help us with ideas or contributions to extend this set. And similarly 
spark users can help us in finding what is missing here by raising challenging tuning questions.   

### How to use Sparklens? ###

#### 1. Using the sparklens package while running your app #### 

Use the following arguments in spark-submit or spark-shell:
```
--packages qubole:sparklens:0.2.0-s_2.11
--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener
```

#### 2. Run from sparklens offline data ####

You can choose not to run sparklens inside the app, but at a later time. Run you app as above 
with an additional conf:
```
--packages qubole:sparklens:0.2.0-s_2.11
--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener
--conf spark.sparklens.reporting.disabled=true
```

This will not run reporting, but instead create a sparklens data file for the application which is 
stored at **spark.sparklens.data.dir** directory (by default it is **/tmp/sparklens/**). This 
data-file can now be used to run sparklens independently, using spark-submit command as follows:

`./bin/spark-submit --packages qubole:sparklens:0.2.0-s_2.11 --class com.qubole.sparklens.app.ReporterApp qubole-dummy-arg <filename>`

`<filename>` should be replaced by the full path of sparklens data file.

#### 3. Run from spark event-history file ####

You can run sparklens on a previously run spark-app using event-history file also, (similar to 
running via sparklens-data-file above) with another option specifying that is file is an 
event-history file. This file can be in any of the formats event-history files supports, i.e. **text, snappy, lz4 
or lzf**. Note the extra `source=history` parameter in this example:

`./bin/spark-submit --packages qubole:sparklens:0.2.0-s_2.11 --class com.qubole.sparklens.app.ReporterApp qubole-dummy-arg <filename> source=history`

#### 4. Checkout the code and use the normal sbt commands: #### 

```
sbt compile 
sbt package 
sbt clean 
```
You will find the Sparklens jar in target/scala-2.11 directory. Make sure scala and java version correspond to those required by your spark cluster. We have tested it with java 7/8, 
scala 2.11.8 and spark versions 2.0.0 onwards. 

Once you have the Sparklens jar available, add the following options to your spark submit command line:
```
--jars /path/to/sparklens_2.11-0.1.0.jar 
--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener
```
You could also add this to your cluster's **spark-defaults.conf** so that it is automatically available for all applications.


### Working with Notebooks ###
It is possible to use Sparklens in your development cycle using Notebooks. Sparklens keeps lots of information in-memory. 
To make it work with Notebooks, it tries to minimize the amount of memory by keeping limited history of jobs executed 
in spark. 

### How to use Sparklens with Python Notebooks (Zeppelin)? ###

1) Add this as first paragraph

```
QNL = sc._jvm.com.qubole.sparklens.QuboleNotebookListener.registerAndGet(sc._jsc.sc())
import time

def profileIt(callableCode, *args):
if (QNL.estimateSize() > QNL.getMaxDataSize()):
  QNL.purgeJobsAndStages()
startTime = long(round(time.time() * 1000))
result = callableCode(*args)
endTime = long(round(time.time() * 1000))
time.sleep(QNL.getWaiTimeInSeconds())
print(QNL.getStats(startTime, endTime))
```
2) wrap your code in some python function say myFunc
3) profileIt(myFunc)

As you can see this is not the only way to use it from python. The core function is:
     **QNL.getStats(startTime, endTime)**

Another way to use this tool, so that we don’t need to worry about objects going out of scope is:

Create the QNL object as part of the first paragraph 
For every piece of code that requires profiling:

```
if (QNL.estimateSize() > QNL.getMaxDataSize()):
  QNL.purgeJobsAndStages()
startTime = long(round(time.time() * 1000))

<-- Your python code here -->

endTime = long(round(time.time() * 1000))
time.sleep(QNL.getWaiTimeInSeconds())
print(QNL.getStats(startTime, endTime))
```

**QNL.purgeJobsAndStages()** is responsible for making sure that the tool doesn’t use too much memory. 
If gives up historical information, throwing away data about old stages to keep the memory usage 
by the tool modest.

### How to use Sparklens with Scala Notebooks (Zeppelin)? ###


1) Add this as first paragraph

```
import com.qubole.sparklens.QuboleNotebookListener
val QNL = new QuboleNotebookListener(sc.getConf)
sc.addSparkListener(QNL)
```
2) Anywhere you need to profile the code:

```
QNL.profileIt {
    //Your code here
}
```

It is important to realize that **QNL.profileIt** takes a block of code as input. Hence any variables declared in this
part are not accessible after the method returns. Of course it can refer to other code/variables in scope. 

The way to go about using this tool with Notebooks is to have only one paragraph in the profiling scope. The moment 
you are happy with the results, just remove the profiling wrapper and execute the same paragraph again. This will ensure 
that your variables come back in scope and are accessible to next paragraph. Also note that, the output of the tool 
in Notebooks is little different from what you would see in command line. This is just to make the information concise. 
We will be making this part configurable. 

### More informtaion? ###
* Introduction to Sparklens https://www.qubole.com/blog/introducing-quboles-spark-tuning-tool/
* Video from meetup. Concepts behind Sparklens https://www.youtube.com/watch?v=0a2U4_6zsCc
* Slides from meetup. https://lnkd.in/fCsrKXj

### Contributing ###
We haven't given much thought. Just raise a PR and if you don't hear from us, shoot an email to 
help@qubole.com to get our attention. 

### Reporting bugs or feature requests ###
Please use the github issues for the Sparklens project to report issues or raise feature requests. If you can code,
better raise a PR.
