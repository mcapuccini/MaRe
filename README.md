# EasyMapReduce

[![Build Status](https://travis-ci.org/mcapuccini/EasyMapReduce.svg?branch=master)](https://travis-ci.org/mcapuccini/EasyMapReduce)

EasyMapReduce leverages the power of Docker and Spark to run and scale your serial tools in MapReduce fashion.

## Table of contents
- [What is EasyMapReduce](#what-is-easymapreduce)
- [Getting Started](#gettign-started)
  - [Get EasyMapReduce](#get-easymapreduce)
  - [Example: DNA GC count (via CLI)](#example-dna-gc-count-via-cli)
  - [Example: DNA GC count (via API)](#example-dna-gc-count-via-api)
- [Multiple input files and whole files](#multiple-input-files-and-whole-files)
- [EasyMapCLI usage](#easymapcli-usage)
- [EasyReduceCLI usage](#easyreducecli-usage)

## What is EasyMapReduce

EasyMapReduce has been developed with scientific application in mind. High-throughput methods produced massive datasets in the past decades, and using frameworks like [Spark](http://spark.apache.org/) and [Hadoop](https://hadoop.apache.org/) is a natural choice to enable high-throughput analysis. In scientific applications, many tools are highly optimized to resemble, or detect some phenomena that occur in a certain system. Hence, sometimes the effort of reimplementing scientific tools in Spark or Hadoop can't be sustained by research groups. EasyMapReduce aims to provide the means to run existing serial tools in MapReduce fashion. Since many of the available scientific tools are trivially parallelizable, [MapReduce](http://research.google.com/archive/mapreduce.html) is an excelent paradigm that can be used to parallelize the computation.

Scientific tools often have many dependencies and, generally speaking, it's difficoult for the system administrator to maintain   software, which may be installed on each node of the cluster, in multiple version. Therefore, instead of running commands straight on the compute nodes, EasyMapReduce starts a user-provided [Docker](https://www.docker.com/) image that wraps a specific tool and all of its dependencies, and it runs the command inside the Docker container. The data goes from Spark through the Docker container, and back to Spark after being processed, via unix files. If the `TMPDIR` environment variable in the worker nodes points to a [tmpfs](https://en.wikipedia.org/wiki/Tmpfs) very little overhead should occur. 

## Gettign started
EasyMapReduce comes as a Spark application that you can submit to an existing Spark cluster. Docker needs to be installed and properly configured on each worker node of the Spark cluster. Also, the user that runs the Spark job needs to be in the docker group.  

### Get EasyMapReduce

EasyMapReduce is distributed through git and it can be built using Maven:

```bash
git clone https://github.com/mcapuccini/EasyMapReduce.git
cd EasyMapReduce
mvn clean package -DskipTests
```

If everything goes well you should find the EasyMapReduce jar (and jar-with-dependencies) in the *target* directory.

### Example: DNA GC count (via CLI)
DNA is a string written in a language of 4 characters: A,T,G,C. Counting how many times G and C occurr in the genome is a task that is often performed in genomics. In this example we use EasyMap and EasyReduce, form the EasyMapReduce package to perform this task in parallel. 

First, we need submit EasyMapCLI to the Spark cluster to count how many times G and C occurr in the file. For simplicity we run Spark in local mode in this example, we suggest you to do the same in your first experiments. 

```
spark-submit --class se.uu.it.easymr.EasyMapCLI \ 
  --master local[*] \
  easymr-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
  --imageName ubuntu:xenial \
  --command 'grep -o [gc] /input | wc -l | tr -d "\n" > /output' \
  /path/to/dna.txt /results/folder/count_by_line.txt
```

**Notes**: 

1. In each container a chunk of the DNA can be read form the `/input` file. This is why we "cat" the /input file in first place in the command

2. The results are read bach in the cluster form the `/output` file. This is why we write the result to the /output file in the command

3. Spark divides the input file (dna.txt) line by line, hence in the result file (count_by_line.txt) there will be the GC count for each line of the file, and not the total sum.

Once we have the GC count line by line, we can use EasyReduceCLI to sum all of the lines together, and the get the total GC count.

```
spark-submit --class se.uu.it.easymr.EasyReduceCLI \
  --master local[*] \
  easymr-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
  --imageName ubuntu:xenial \
  --command 'expr $(cat /input1) + $(cat /input2) | tr -d "\n" > /output' \
  /results/folder/count_by_line.txt /results/folder/sum.txt
```

**Notes**: 

1. The goal of a reduce task is to combine multiple chunks of the data together. I each container two splits of the data are available in the `/input1` and `/input2` file. Like in EasyMap the result needs to be written in `/output`

2. For the final result to be correct, and in many case for the reduce task to succeed, the provided command needs to be associative and commutative. Please give a look [here](http://stackoverflow.com/questions/329423/parallelizing-the-reduce-in-mapreduce) for more details

We suggest you to repeat this experiment yourself, using the example files in this [repository](https://github.com/mcapuccini/EasyMapReduce/tree/master/src/test/resources/se/uu/it/easymr/dna).

### Example: DNA GC count (via API)
EasyMapReduce aslo comes along with a Scala API, so you can use it in your Spark applications. The equivalent of the previous example using the API follows.

```scala
val rdd = sc.textFile(getClass.getResource("dna/dna.txt").getPath)
val count = new EasyMapReduce(rdd)
 .mapPartitions(
    imageName = "ubuntu:xenial",
    command = "grep -o '[gc]' /input | wc -l | tr -d '\\n' > /output")
 .reduce(
    imageName = "ubuntu:xenial",
    command = "expr $(cat /input1) + $(cat /input2) | tr -d '\\n' > /output")
```

For more details please refer to the [unit tests](https://github.com/mcapuccini/EasyMapReduce/tree/master/src/test/scala/se/uu/it/easymr).

## Multiple input files and whole files
In many scientific applications, instead of having a single big file, there are many smaller files that need to be processed by the command all together without being splitted line by line. If this is you use case please give a look to the `---wholeFiles` option in the usage sections. 

## EasyMapCLI usage
```
EasyMap: it maps a distributed dataset using a command form a Docker container.
Usage: Easy Map [options] inputPath outputPath

  --imageName <value>  Docker image name.
  --command <value>    command to run inside the Docker container, e.g. 'rev /input > /output | tr -d "\n"'.
  --wholeFiles         if set, multiple input files will be loaded from an input directory. The command will executed in parallel, on the whole files. In contrast, when this is not set the file/files in input is/are splitted line by line, and the command is executed in parallel on each line of the file.
  --local              set to run in local mode (useful for testing purpose).
  inputPath            dataset input path. Must be a directory if wholeFiles is set.
  outputPath           results output path.
```

## EasyReduceCLI usage
```
EasyReduce: reduce a distributed dataset using a command from a Docker container.
Usage: EasyReduce [options] inputPath outputPath

  --imageName <value>  Docker image name.
  --command <value>    command to run inside the Docker container, e.g. 'expr $(cat /input1) + $(cat /input2) | tr -d "\n" > /output'. The command needs to be associative and commutative.
  --wholeFiles         if set, multiple input files will be loaded from an input directory. The command will executed in parallel, on the whole files. In contrast, when this is not set the file/files in input is/are splitted line by line, and the command is executed in parallel on each line of the file.
  --local              set to run in local mode (useful for testing purpose).
  inputPath            dataset input path. Must be a directory if wholeFiles is set.
  outputPath           result output path
```
