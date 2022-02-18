# Introduction
This is an example project that shows how to use `flink-connector` project to write data to a Delta table using Apache Flink.

## Run example for non-partitioned Delta table
To run example in-memory Flink job writing data a non-partitioned Delta table run:

- with your local IDE:
Simply run `io.delta.flink.example.sink.DeltaSinkExample` class that contains `main` method
- with Maven:
> cd examples/flink-example/
> 
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=io.delta.flink.example.sink.DeltaSinkExample
- with SBT:
> cd examples/
>
> build/sbt "flinkExample/runMain io.delta.flink.example.sink.DeltaSinkExample"

## Run example for partitioned Delta table
To run example in-memory Flink job writing data a non-partitioned Delta table run:

- with your local IDE:
  Simply run `io.delta.flink.example.sink.DeltaSinkPartitionedTableExample` class that contains `main` method
- with Maven:
> cd examples/flink-example/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=io.delta.flink.example.sink.DeltaSinkPartitionedTableExample
- with SBT:
> cd examples/
>
> build/sbt "flinkExample/runMain io.delta.flink.example.sink.DeltaSinkPartitionedTableExample"

## Verify
After performing above steps you may observe your command line that will be printing descriptive information
about produced data. Streaming Flink job will run until manual termination and will be producing 1 event
in the interval of 800 millis by default.

To inspect written data look inside `examples/flink-example/src/main/resources/example_table` or
`examples/flink-example/src/main/resources/example_partitioned_table` which will contain created Delta tables along with the written Parquet files.

NOTE: there is no need to manually delete previous data to run the example job again - the example application will do it automatically
