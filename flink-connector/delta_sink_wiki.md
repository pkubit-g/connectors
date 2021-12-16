# Flink DeltaSink

The Flink DeltaSink is a Java library that can be used to construct Apache Flink's sink object for
writing data to a DeltaLake tables. It uses
Flink's [new Sink API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-143%3A+Unified+Sink+API)
by exposing methods for creating instance of `org.apache.flink.api.connector.sink.Sink` class (which
in our case is `io.delta.flink.DeltaSink` class) that can be further added as a sink to Flink job's
streaming environment.

## Use Cases

You can use this connector if you want to write data from Apache Flink's jobs to Delta's tables.

### Caveats

Currently, we are not providing any small files' compaction capabilities nor we are providing any
mechanisms for cleaning up uncommitted files. E.g. if your Flink job has been processing some data
and generated some in-progress files (files that have been created on file system but haven't been
yet committed to the DeltaLog) and it was terminated in a non-graceful way then you should clean
those files manually.

## APIs

Flink DeltaSink exposes utility static methods for creating an instance
of `org.apache.flink.api.connector.sink.Sink` capable of writing data from Flink job to a Delta
table by providing exactly-once delivery guarantees. For more details, please see
the [Java API docs](https://delta-io.github.io/connectors/latest/flink-connector/api/java/index.html)
.

#### DeltaSink

[DeltaSink](https://delta-io.github.io/connectors/latest/flink-connector/api/java/io/delta/flink/DeltaSink.html)
is the main class for creating the sink instance programmatically. It extends
Flink's `org.apache.flink.api.connector.sink.Sink` class that should be passed as an input argument
to `org.apache.flink.streaming.api.datastream.DataStream.sinkTo(Sink<T, ?, ?, ?> sink)` method
responsible for plugging the sink to the stream. `DeltaSink` object is responsible for creating sink
writers to a Delta table and managing checkpointing behaviour (like generating Delta transactional
id and providing snapshotting information to Flink framework). To create `DeltaSink` instance you
can use static utility method `DeltaSink.builderFor` method that returns a builder instance, but you
can also create the builder directly.

#### DeltaSource

Will be provided in future releases.

## API Compatibility

Users should rely on classes that are not annotated with a `@Internal` mark. Those classes are
exposes on purpose and should not be subject to modifications in future releases (but will be
subject to some extensions while keeping it backwards compatible).

## Add Flink Delta Connector to your project

You can add the Flink Connector library as a dependency using your favorite build tool. Please note
that it expects packages:

- `delta-standalone`
- `flink-runtime`
- `flink-table-runtime-blink` (only for Flink <= 1.13.x)
- `flink-table-runtime` (only for Flink >= 1.14.x)
- `flink-table-common`
- `flink-parquet`
- `hadoop-client`

to be provided. Please see the following build files for more details.

### Maven

Please replace the versions of the dependencies with the ones you are using.

Scala 2.12:

```xml

<project>
    <dependencies>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>flink-connector</artifactId>
            <version>0.2.1-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>delta-standalone_2.11</artifactId>
            <version>0.2.1-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-runtime-blink_2.11</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-parquet_2.11</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.1.0</version>
        </dependency>
    </dependencies>
</project>
```

Scala 2.11:

```xml

<project>
    <dependencies>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>flink-connector</artifactId>
            <version>0.2.1-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>delta-standalone_2.11</artifactId>
            <version>0.2.1-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-runtime-blink_2.11</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-parquet_2.11</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop-version}</version>
        </dependency>
    </dependencies>
</project>
```

NOTE: if you want to use Flink >= 1.14.0 then instead

```xml

<dependencies>
    ...
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-runtime-blink_2.12</artifactId>
        <version>${flink-version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-runtime_2.12</artifactId>
        <version>${flink-version}</version>
    </dependency>
    ...
</dependencies>
```

you should provide:

```xml

<dependencies>
    ...
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-table-runtime_2.12</artifactId>
        <version>${flink-version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-runtime</artifactId>
        <version>${flink-version}</version>
    </dependency>
    ...
</dependencies>
```

### SBT

Please replace the versions of the dependencies with the ones you are using.

```
libraryDependencies ++= Seq(
  "io.delta" %% "flink-connector" % "0.2.1-SNAPSHOT",
  "io.delta" %% "delta-standalone" % "0.2.1-SNAPSHOT",
  "org.apache.flink" %% "flink-runtime" % flinkVersion,
  "org.apache.flink" %% "flink-table-runtime-blink" % flinkVersion,
  "org.apache.flink" % "flink-table-common" % flinkVersion,
  "org.apache.flink" %% "flink-parquet" % flinkVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion)
```

## Environment Requirements

- JDK 8 or above.
- Scala 2.11 or 2.12.

## Usage

### 1. Sink Creation

In this example we show how to create a `DeltaSink` and plug it to an
existing `org.apache.flink.streaming.api.datastream.DataStream`.

```java
package com.example;

import org.apache.flink.connector.delta.sink.DeltaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class DeltaSinkExample {

    public DataStream<RowData> createDeltaSink(DataStream<RowData> stream,
                                               String deltaTablePath,
                                               RowType rowType) {
        DeltaSink<RowData> deltaSink =
            DeltaSink.forDeltaFormat(new Path(deltaTablePath), rowType).build();
        stream.sinkTo(deltaSink);
        return stream;
    }
}
```

### 2. Sink Creation for partitioned tables

In this example we show how to create a `DeltaSink` and
implement `io.delta.flink.DeltaTablePartitionAssigner` that will enable writing data to a
partitioned table.

```java
package com.example;

import org.apache.flink.connector.delta.sink.DeltaSink;
import org.apache.flink.connector.delta.sink.DeltaSinkBuilder;
import org.apache.flink.connector.delta.sink.DeltaTablePartitionAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.LinkedHashMap;

public class DeltaSinkExample {

    public static final RowType ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("surname", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("age", new IntType())
    ));

    public DataStream<RowData> createDeltaSink(DataStream<RowData> stream,
                                               String deltaTablePath) {
        DeltaTablePartitionAssigner<RowData> partitionAssigner =
            new DeltaTablePartitionAssigner<>(new MultiplePartitioningColumnComputer());

        DeltaSinkBuilder<RowData> deltaSinkBuilder =
            DeltaSink.forDeltaFormat(new Path(deltaTablePath), ROW_TYPE);
        deltaSinkBuilder.withBucketAssigner(partitionAssigner);
        DeltaSink<RowData> deltaSink = deltaSinkBuilder.build();

        stream.sinkTo(deltaSink);
        return stream;
    }

    static class MultiplePartitioningColumnComputer implements
        DeltaTablePartitionAssigner.DeltaPartitionComputer<RowData> {

        @Override
        public LinkedHashMap<String, String> generatePartitionValues(
            RowData element, BucketAssigner.Context context) {
            String name = element.getString(0).toString();
            int age = element.getInt(2);
            LinkedHashMap<String, String> partitionSpec = new LinkedHashMap<>();
            partitionSpec.put("name", name);
            partitionSpec.put("age", Integer.toString(age));
            return partitionSpec;
        }
    }
}
```

### 3. Example job with `DeltaSink` from `flink-connector`.

In this example we show an example Flink job using `DeltaSink`.

NOTE: Please remember that in order to run it locally you also need to attach `flink-clients` module
to your build dependencies.

```java
package com.example;

import org.apache.flink.connector.delta.sink.DeltaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DeltaSinkExample {

    public static String TABLE_PATH = "/my/table/path";

    public static final RowType ROW_TYPE = new RowType(Arrays.asList(
        new RowType.RowField("name", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("surname", new VarCharType(VarCharType.MAX_LENGTH)),
        new RowType.RowField("age", new IntType())
    ));

    public static void main(String[] args) throws Exception {
        new DeltaSinkExample().run();
    }

    void run() throws Exception {
        DeltaSink<RowData> deltaSink =
            DeltaSink.forDeltaFormat(new Path(TABLE_PATH), ROW_TYPE).build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromCollection(getTestRowData(100))
            .sinkTo(deltaSink);
        env.execute();
    }

    private List<RowData> getTestRowData(int num_records) {
        DataFormatConverters.DataFormatConverter<RowData, Row> converter =
            DataFormatConverters.getConverterForDataType(
                TypeConversions.fromLogicalToDataType(ROW_TYPE));
        List<RowData> rows = new ArrayList<>(num_records);
        for (int i = 0; i < num_records; i++) {
            Integer v = i;
            RowData rowData = converter.toInternal(
                Row.of(String.valueOf(v), String.valueOf((v + v)), v));
            rows.add(rowData);
        }
        return rows;
    }
}
```