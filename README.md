# EasyMapReduce

[![Build Status](https://travis-ci.org/mcapuccini/EasyMapReduce.svg?branch=master)](https://travis-ci.org/mcapuccini/EasyMapReduce)

EasyMapReduce leverages the power of Docker and Spark to run and scale your serial tools in MapReduce fashion.

**20 minutes introduction video**:

[![youtube](https://img.youtube.com/vi/4C4R9qptUQo/0.jpg)](https://www.youtube.com/watch?v=4C4R9qptUQo)

## Table of contents
- [What is EasyMapReduce](#what-is-easymapreduce)
- [Getting Started](#gettign-started)
  - [Get EasyMapReduce](#get-easymapreduce)
- [Example: DNA GC count](#example-dna-gc-count)

## What is EasyMapReduce

EasyMapReduce has been developed with scientific application in mind. High-throughput methods produced massive datasets in the past decades, and using frameworks like [Spark](http://spark.apache.org/) and [Hadoop](https://hadoop.apache.org/) is a natural choice to enable high-throughput analysis. In scientific applications, many tools are highly optimized to resemble, or detect some phenomena that occur in a certain system. Hence, sometimes the effort of reimplementing scientific tools in Spark or Hadoop can't be sustained by research groups. EasyMapReduce aims to provide the means to run existing serial tools in MapReduce fashion. Since many of the available scientific tools are trivially parallelizable, [MapReduce](http://research.google.com/archive/mapreduce.html) is an excellent paradigm that can be used to parallelize the computation.

Scientific tools often have many dependencies and, generally speaking, it's difficult for the system administrator to maintain   software, which may be installed on each node of the cluster, in multiple version. Therefore, instead of running commands straight on the compute nodes, EasyMapReduce starts a user-provided [Docker](https://www.docker.com/) image that wraps a specific tool and all of its dependencies, and it runs the command inside the Docker container. The data goes from Spark through the Docker container, and back to Spark after being processed, via Unix files. If the `TMPDIR` environment variable in the worker nodes points to a [tmpfs](https://en.wikipedia.org/wiki/Tmpfs) very little overhead should occur. 

## Gettign started
EasyMapReduce comes as a Scala library that you can use in your Spark applications. Please keep in mind that when submitting EasyMapReduce applications, Docker needs to be installed and properly configured on each worker node of your Spark cluster. Also, the user that runs the Spark job needs to be in the Docker group.  

### Get EasyMapReduce

EasyMapReduce is distributed through Git and it can be built using Maven:

```bash
git clone https://github.com/mcapuccini/EasyMapReduce.git
cd EasyMapReduce
mvn clean package -DskipTests
```

If everything goes well you should find the EasyMapReduce jar in the *target* directory.

## Example: DNA GC count 
DNA can be represented as a string written in a language of 4 characters: A,T,G,C. Counting how many times G and C occur in a genome is a task that is often performed in genomics. In this example we use EasyMapReduce to perform this task in parallel with POSIX commands. 

```scala
val rdd = sc.textFile("genome.txt")
val res = new EasyMapReduce(rdd)
	.map(
    	imageName = "ubuntu:xenial",
        command = "grep -o '[gc]' /input | wc -l > /output")
    .reduce(
    	imageName = "ubuntu:xenial",
        command = "awk '{s+=$1} END {print s}' /input > /output")
println(s"The GC count is: $res")
```
