# EasyMapReduce

EasyMapReduce leverages the power of Docker and Spark to run and scale your serial tools in MapReduce fashion.

EasyMapReduce has been developed with scientific application in mind. High-throughput methods produced massive datasets in the past decades, and using frameworks like [Spark](http://spark.apache.org/) and [Hadoop](https://hadoop.apache.org/) is a natural choice to enable high-throughput analysis. In scientific applications, many tools are highly optimized to resemble, or detect some phenomena that occur in a certain system. Hence, sometimes the effort of reimplementing scientific tools in Spark or Hadoop can't be sustained by research groups. EasyMapReduce aims to provide the means to run existing serial tools in MapReduce fashion. Since many of the available scientific tools are trivially parallelizable, [MapReduce](http://research.google.com/archive/mapreduce.html) is an excelent paradigm that can be used to parallelize the computation.

Scientific tools often have many dependencies and, generally speaking, it's difficoult for the system administrator to maintain   software, which may be installed on each node of the cluster, in multiple version. Therefore, instead of running commands straight on the compute nodes, EasyMapReduce starts a user-provided [Docker](https://www.docker.com/) image that wraps a specific tool and all of its dependencies, and it runs the command inside the Docker container. The data goes from Spark through the Docker container, and back to Spark after being processed, via unix named pipes, hence very little overhead occurs.

## Gettign started
EasyMapReduce comes as a Spark application that you can submit to an existing Spark cluster. Docker needs to be installed and properly configured on each worker node of the Spark cluster. Also, the user that runs the Spark job needs to be in the docker group.  

You can download the latest build with all of the dependencies here:
[download](http://pele.farmbio.uu.se/artifactory/libs-release/se/uu/farmbio/easymr/0.0.1/easymr-0.0.1-jar-with-dependencies.jar).

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
