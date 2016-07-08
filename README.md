# EasyMapReduce

EasyMapReduce leverages the power of Docker and Spark to run and scale your serial tools in MapReduce fashion.

EasyMapReduce has been developed with scientific application in mind. High-throughput methods produced massive datasets in the past decades, and using frameworks like [Spark](http://spark.apache.org/) and [Hadoop](https://hadoop.apache.org/) is a natural choice to enable high-throughput analysis. In scientific applications, many tools are highly optimized to resemble, or detect some phenomena that occur in a certain system. Hence, sometimes the effort of reimplementing scientific tools in Spark or Hadoop can't be sustained by research groups. EasyMapReduce aims to provide the means to run existing serial tools in MapReduce fashion. Since many of the available scientific tools are trivially parallelizable, [MapReduce](http://research.google.com/archive/mapreduce.html) is an excelent paradigm that can be used to parallelize the computation.

Scientific tools often have many dependencies and, generally speaking, it's difficoult for the system administrator to maintain   software, which may be installed on each node of the cluster, in multiple version. Therefore, instead of running commands straight on the compute nodes, EasyMapReduce starts a user-provided [Docker](https://www.docker.com/) image that wraps a specific tool and all of its dependencies, and it runs the command inside the Docker container. The data goes from Spark through the Docker container, and back to Spark after being processed, via unix named pipes, hence very little overhead occurs.

## Gettign started
EasyMapReduce comes as a Spark application that you can submit to an existing Spark cluster. Docker needs to be installed and properly configured on each worker node of the Spark cluster. Also, the user that runs the Spark job needs to be in the docker group.  

You can download the latest build with all of the dependencies here:
[download](http://pele.farmbio.uu.se/artifactory/libs-release/se/uu/farmbio/easymr/0.0.1/easymr-0.0.1-jar-with-dependencies.jar).

### Example: DNA GC count
DNA is a string written in a language of 4 characters: A,T,G,C. Counting how many times G and C occurr in the genome is a task that is often performed in genomics. In this example we use EasyMap and EasyReduce, form the EasyMapReduce package to perform this task in parallel. 

First, we need submit EasyMap to the Spark cluster to count how many times G and C occurr in the file. For simplicity we run Spark in local mode in this example, we suggest you to do the same in your first experiments. 

```
spark-submit --class se.uu.farmbio.easymr.EasyMap \ 
  --master local[*] \
  easymr-0.0.1.jar \
  --imageName ubuntu:14.04 \
  --command "cat /input | fold -1 | grep [gc] | wc -l > /output" \
  /path/to/dna.txt /results/foler/count_by_line.txt
```

**Notes**: 

1. In each container a chunk of the DNA can be read form the `/input` file. This is why we "cat" the /input file in first place in the command

2. The results are read bach in the cluster form the `/output` file. This is why we write the result to the /output file in the command

3. Spark divides the input file (dna.txt) line by line, hence in the result file (count_by_line.txt) there will be the GC count for each line of the file, and not the total sum.

Once we have the GC count line by line, we can use EasyReduce to sum all of the lines together, and the get the total GC count.

```
spark-submit --class se.uu.farmbio.easymr.EasyReduce \
  --master local[*] \
  easymr-0.0.1.jar \
  --imageName ubuntu:14.04 \
  --command 'expr $(cat /input1) + $(cat /input2) > /output' \
  /results/foler/count_by_line.txt /results/foler/sum.txt
```

**Notes**: 

1. The goal of a reduce task is to combine multiple chunks of the data together. I each container two splits of the data are available in the `/input1` and `/input2` file. Like in EasyMap the result needs to be written in `/output`

2. For the final result to be correct, and in many case for the reduce task to succeed, the provided command needs to be associative and commutative. Please give a look [here](http://stackoverflow.com/questions/329423/parallelizing-the-reduce-in-mapreduce) for more details

We suggest you to repeat this experiment yourself, using the example files in this [repository](https://github.com/mcapuccini/EasyMapReduce/tree/master/src/test/resources/se/uu/farmbio/easymr/dna).

### Multiple input files and whole files
In many scientific applications, instead of having a single big file, there are many smaller files that need to be processed by the command all together without being splitted line by line. If this is you use case please give a look to the --wholeFiles option in the usage sections. 

## EasyMap usage
```
Usage: Easy Map [options] inputPath outputPath

  --imageName <value>
        Docker image name (default: "ubuntu:14.04").
  --command <value>
        command to run inside the Docker container, e.g. 'rev /input > /output'.
  --noTrim
        if set the command output will not get trimmed.
  --wholeFiles
        if set, multiple input files will be loaded from an input directory. 
        The command will executed in parallel, on the whole files. In contrast, 
        when this is not set the file/files in input is/are splitted line by line, 
        and the command is executed in parallel on each line of the file.
  --commandTimeout <value>
        execution timeout for the command, in sec. (default: 1200).
  --local
        set to run in local mode (useful for testing purpose).
  inputPath
        dataset input path. Must be a directory if wholeFiles is set.
  outputPath
        result output path.
```

## EasyReduce usage
```
Usage: EasyReduce [options] inputPath outputPath

  --imageName <value>
        Docker image name (default: "ubuntu:14.04").
  --command <value>
        command to run inside the Docker container, e.g. 'expr sum $(cat /input1) + $(cat /input2) > /output'. 
        The command needs to be associative and commutative.
  --noTrim
        if set the command output will not get trimmed.
  --wholeFiles
        if set, multiple input files will be loaded from an input directory. 
        The command will executed in parallel, on the whole files. In contrast, 
        when this is not set the file/files in input is/are splitted line by line, 
        and the command is executed in parallel on each line of the file.
  --commandTimeout <value>
        execution timeout for the command, in sec. (default: 1200).
  --local
        set to run in local mode (useful for testing purpose).
  inputPath
        dataset input path. Must be a directory if wholeFiles is set.
  outputPath
        result output path.
```
