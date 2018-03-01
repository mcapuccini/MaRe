# MaRe

> Italian, pronounced: `/ˈmare/`. Noun: `Sea`.

[![Build Status](https://travis-ci.org/mcapuccini/MaRe.svg?branch=master)](https://travis-ci.org/mcapuccini/MaRe)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/se.uu.it/mare/badge.svg)](https://maven-badges.herokuapp.com/maven-central/se.uu.it/mare)


MaRe (formerly EasyMapReduce) leverages the power of Docker and Spark to run and scale your serial tools in MapReduce fashion.

**20 minutes introduction video**:

[![youtube](https://img.youtube.com/vi/4C4R9qptUQo/0.jpg)](https://www.youtube.com/watch?v=4C4R9qptUQo)

## Table of contents
- [What is MaRe](#what-is-mare)
- [Example: DNA GC count](#example-dna-gc-count)
- [Getting Started](#getting-started)
  - [Get MaRe](#get-mare)
  - [Documentation](#documentation)

## What is MaRe

MaRe has been developed with scientific application in mind. High-throughput methods produced massive datasets in the past decades, and using frameworks like [Spark](http://spark.apache.org/) and [Hadoop](https://hadoop.apache.org/) is a natural choice to enable high-throughput analysis. In scientific applications, many tools are highly optimized to resemble, or detect some phenomena that occur in a certain system. Hence, sometimes the effort of reimplementing scientific tools in Spark or Hadoop can't be sustained by research groups. MaRe aims to provide the means to run existing serial tools in MapReduce fashion. Since many of the available scientific tools are trivially parallelizable, [MapReduce](http://research.google.com/archive/mapreduce.html) is an excellent paradigm that can be used to parallelize the computation.

Scientific tools often have many dependencies and, generally speaking, it's difficult for the system administrator to maintain   software, which may be installed on each node of the cluster, in multiple version. Therefore, instead of running commands straight on the compute nodes, MaRe starts a user-provided [Docker](https://www.docker.com/) image that wraps a specific tool and all of its dependencies, and it runs the command inside the Docker container. The data goes from Spark through the Docker container, and back to Spark after being processed, via Unix files. If the `TMPDIR` environment variable in the worker nodes points to a [tmpfs](https://en.wikipedia.org/wiki/Tmpfs) very little overhead should occur. 

## Example: DNA GC count 
DNA can be represented as a string written in a language of 4 characters: A,T,G,C. Counting how many times G and C occur in a genome is a task that is often performed in genomics. In this example we use MaRe to perform this task in parallel with POSIX commands. 

```scala
val rdd = sc.textFile("genome.dna")
val res = new MaRe(rdd)
    .setInputMountPoint("/input.dna")
    .setOutputMountPoint("/output.dna")
    .mapPartitions(
    	imageName = "ubuntu:xenial",
      	command = "grep -o '[gc]' /input.dna | wc -l > /output.dna")
    .reducePartitions(
        imageName = "ubuntu:xenial",
        command = "awk '{s+=$1} END {print s}' /input.dna > /output.dna")
println(s"The GC count is: $res")
```

## Getting started
MaRe comes as a Scala library that you can use in your Spark applications. Please keep in mind that when submitting MaRe applications, Docker needs to be installed and properly configured on each worker node of your Spark cluster. Also, the user that runs the Spark job needs to be in the Docker group.  

### Get MaRe

MaRe is packaged and distributed with Maven, all you have to do is to add its dependency to your pom.xml file:

```xml
<dependencies>
  ...
  <dependency>
    <groupId>se.uu.it</groupId>
    <artifactId>mare</artifactId>
    <version>0.3.0</version>
  </dependency>
  ...
</dependencies>
```

### Documentation

API documentation is available here: https://mcapuccini.github.io/MaRe/scaladocs/.
