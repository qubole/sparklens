[![Gitter](https://badges.gitter.im/qubole-sparklens/community.svg)](https://gitter.im/qubole-sparklens/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

# README #

Sparklens is a profiling tool for Spark with a built-in Spark scheduler simulator. Its primary goal is to make it easy 
to understand the scalability limits of Spark applications. It helps in understanding how efficiently a given 
Spark application is using the compute resources provided to it. Maybe your application will run faster with more 
executors and may be it wont. Sparklens can answer this question by looking at a single run of your application. 

It helps you narrow down to few stages (or driver, or skew or lack of tasks) which are limiting your application 
from scaling out and provides contextual information about what could be going wrong with these stages. Primarily 
it helps you approach spark application tuning as a well defined method/process instead of something you learn by 
trial and error, saving both developer and compute time. 

## Sparklens Reporting as a Service

http://sparklens.qubole.com is a reporting service built on top of Sparklens. This service was built to lower the pain of sharing and discussing Sparklens 
output. Users can upload the Sparklens JSON file to this service and retrieve a global sharable 
link. The link delivers the Sparklens report in an easy-to-consume HTML format with intuitive 
charts and animations. It is also useful to have a link for easy reference for yourself, in case 
some code changes result in lower utilization or make the application slower.

## What does it report?

* Estimated completion time and estimated cluster utilisation with different numbers of executors
 
 ```
 Executor count    31  ( 10%) estimated time 87m 29s and estimated cluster utilization 92.73%
 Executor count    62  ( 20%) estimated time 47m 03s and estimated cluster utilization 86.19%
 Executor count   155  ( 50%) estimated time 22m 51s and estimated cluster utilization 71.01%
 Executor count   248  ( 80%) estimated time 16m 43s and estimated cluster utilization 60.65%
 Executor count   310  (100%) estimated time 14m 49s and estimated cluster utilization 54.73%
```
Given a single run of a Spark application, Sparklens can estimate how your application will perform 
given any arbitrary number of executors. This helps you understand the ROI on adding executors. 

* Job/stage timeline which shows how the parallel stages were scheduled within a job. This makes it easy to visualise 
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

*Lots of interesting per-stage metrics like Input, Output, Shuffle Input and Shuffle Output per stage. **OneCoreComputeHours** 
available and used per stage to discover inefficient stages. 

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

Internally, Sparklens has the concept of an analyzer which is a generic component for emitting interesting events. 
The following analyzers are currently available:

1. AppTimelineAnalyzer
2. EfficiencyStatisticsAnalyzer
3. ExecutorTimelineAnalyzer
4. ExecutorWallclockAnalyzer
5. HostTimelineAnalyzer
6. JobOverlapAnalyzer
7. SimpleAppAnalyzer
8. StageOverlapAnalyzer
9. StageSkewAnalyzer

We are hoping that Spark experts the world over will help us with ideas or contributions to extend this set. Similarly, 
Spark users can help us in finding what is missing here by raising challenging tuning questions.   

## How to use Sparklens?

#### 1. Using the Sparklens package while running your app #### 

Note: Apart from the console based report, you can also get an UI based report similar to 
[this](http://sparklens.qubole.com/report_view/1b3868a49388e7ab6a16) in your email. You have to pass
 `--conf spark.sparklens.report.email=<email>` along with other relevant confs mentioned below.
 This functionality is available in Sparklens 0.3.2 and above.  

Use the following arguments to `spark-submit` or `spark-shell` for spark-3.0.0 and latest version of spark:
```
--packages qubole:sparklens:0.4.0-s_2.12
--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener
```

Use the following arguments to `spark-submit` or `spark-shell` for spark-2.4.x and lower version of spark:
```
--packages qubole:sparklens:0.3.2-s_2.11
--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener
```


#### 2. Run from Sparklens offline data ####

You can choose not to run sparklens inside the app, but at a later time. Run your app as above 
with additional configuration parameters
For spark-3.0.0 and latest version of spark:
```
--packages qubole:sparklens:0.4.0-s_2.12
--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener
--conf spark.sparklens.reporting.disabled=true
```

For spark-2.4.x and lower version of spark:
```
--packages qubole:sparklens:0.3.2-s_2.11
--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener
--conf spark.sparklens.reporting.disabled=true
```

This will not run reporting, but instead create a Sparklens JSON file for the application which is 
stored in the **spark.sparklens.data.dir** directory (by default, **/tmp/sparklens/**). Note that this will be stored on HDFS by default. To save this file to s3, please set **spark.sparklens.data.dir** to s3 path. This data file can now be used to run Sparklens reporting independently, using `spark-submit` command as follows:

`./bin/spark-submit --packages qubole:sparklens:0.4.0-s_2.12 --class com.qubole.sparklens.app.ReporterApp qubole-dummy-arg <filename>`

`<filename>` should be replaced by the full path of sparklens json file. If the file is on s3 use the full s3 path. For files on local file system, use file:// prefix with the local file location. HDFS is supported as well. 

You can also upload a Sparklens JSON data file to http://sparklens.qubole.com to see this report as an HTML page. 

#### 3. Run from Spark event-history file ####

You can also run Sparklens on a previously run spark-app using an event history file, (similar to 
running via `sparklens-json-file` above) with another option specifying that is file is an 
event history file. This file can be in any of the formats the event history files supports, i.e. **text, snappy, lz4 
or lzf**. Note the extra `source=history` parameter in this example:

`./bin/spark-submit --packages qubole:sparklens:0.4.0-s_2.12 --class com.qubole.sparklens.app.ReporterApp qubole-dummy-arg <filename> source=history`

It is also possible to convert an event history file to a Sparklens json file using the following command:

`./bin/spark-submit --packages qubole:sparklens:0.4.0-s_2.12 --class com.qubole.sparklens.app.EventHistoryToSparklensJson qubole-dummy-arg <srcDir> <targetDir>`

EventHistoryToSparklensJson is designed to work on local file system only. Please make sure that the source and target directories are on local file system.

#### 4. Checkout the code and use the normal sbt commands: #### 

```
sbt compile 
sbt package 
sbt clean 
```
You will find the Sparklens jar in the `target/scala-2.11` directory. Make sure the Scala and Java versions correspond to those required by your Spark cluster. We have tested it with Java 7/8, 
Scala 2.11.8 and Spark versions 2.0.0 and onwards. 

Once you have the Sparklens JAR available, add the following options to your `spark-submit` command line:
```
--jars /path/to/sparklens_2.11-0.3.2.jar 
--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener
```
You could also add this to your cluster's **spark-defaults.conf** so that it is automatically available for all applications.


## Working with Notebooks
It is possible to use Sparklens in your development cycle using notebooks. Sparklens keeps lots of information in-memory. 
To make it work with notebooks, it tries to minimize the amount of memory by keeping limited history of jobs executed 
in Spark.

## How to use Sparklens with Python notebooks (e.g. Zeppelin)

1) Add this as the first cell

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
2) Wrap your code in some python function say myFunc
3) `profileIt(myFunc)`

As you can see this is not the only way to use it from Python. The core function is:
     **QNL.getStats(startTime, endTime)**

Another way to use this tool, so that we don’t need to worry about objects going out of scope is:

Create the QNL object as part of the first paragraph 
For every piece of code that requires profiling:

```
if (QNL.estimateSize() > QNL.getMaxDataSize()):
  QNL.purgeJobsAndStages()
startTime = long(round(time.time() * 1000))

<-- Your Python code here -->

endTime = long(round(time.time() * 1000))
time.sleep(QNL.getWaiTimeInSeconds())
print(QNL.getStats(startTime, endTime))
```

`QNL.purgeJobsAndStages()` is responsible for making sure that the tool doesn’t use too much memory. 
It removes historical information, throwing away data about old stages to keep the memory usage 
by the tool modest.

## How to use Sparklens with Scala notebooks (e.g. Zeppelin)


1) Add this as the first cell

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

It is important to realize that `QNL.profileIt` takes a block of code as input. Hence any variables declared in this
part are not accessible after the method returns. Of course it can refer to other code/variables in scope. 

The way to go about using this tool with notebooks is to have only one cell in the profiling scope. The moment 
you are happy with the results, just remove the profiling wrapper and execute the same cell again. This will ensure 
that your variables come back in scope and are accessible to next cell. Also note that, the output of the tool 
in notebooks is little different from what you would see in command line. This is just to make the information concise. 
We will be making this part configurable. 

## More informtaion?
* [Introduction to Sparklens](https://www.qubole.com/blog/introducing-quboles-spark-tuning-tool/)
* [Video from meetup: Concepts behind Sparklens](https://www.youtube.com/watch?v=0a2U4_6zsCc)
* [Slides from meetup](https://lnkd.in/fCsrKXj)
* [Video from Fifth Elephant Conference](https://www.youtube.com/watch?v=SOFztF-3GGk)
* [Video from Spark AI Summit London 2018](https://www.youtube.com/watch?v=KS5vRZPLo6c)

## Release Notes
- [03/20/2018] Version 0.1.1 - Sparklens Core
- [04/06/2018] Version 0.1.2 - Package name fixes
- [08/07/2018] Version 0.2.0 - Support for offline reporting
- [01/10/2019] Version 0.2.1 - Stability fixes
- [05/10/2019] Version 0.3.0 - Support for handling parallel Jobs
- [05/10/2019] Version 0.3.1 - Fixed JSON parsing issue with Spark 2.4.0 and above
- [05/06/2020] Version 0.3.2 - Support for generating email based report using sparklens.qubole.com

## Contributing
We haven't given this much thought. Just raise a PR and if you don't hear from us, shoot an email to 
[help@qubole.com](mailto:help@qubole.com) to get our attention. 

## Reporting bugs or feature requests
Please use the GitHub issues for the Sparklens project to report issues or raise feature requests. If you can code,
better raise a PR.
