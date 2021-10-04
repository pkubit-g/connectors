package org.apache.flink.connector.delta.sink.writer.parquet;

import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.utils.SerializableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

import static org.apache.parquet.hadoop.ParquetOutputFormat.MAX_PADDING_BYTES;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getDictionaryPageSize;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getEnableDictionary;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getPageSize;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getValidation;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getWriterVersion;

// IN PROGRESS, not used currently, not sure if needed
public class DeltaParquetWriterBuilder<T> extends ParquetWriter.Builder<T, DeltaParquetWriterBuilder<T>> {

    WriteSupport<T> writeSupport;

    public DeltaParquetWriterBuilder(OutputFile path,
                                     WriteSupport<T> writeSupport) {
        super(path);
        this.writeSupport = writeSupport;
    }

    @Override
    protected DeltaParquetWriterBuilder<T> self() {
        return this;
    }

    @Override
    protected WriteSupport<T> getWriteSupport(Configuration conf) {
        return this.writeSupport;
    }


    public static <T> ParquetWriterFactory<T> createWriterFactory(
            Configuration conf,
            WriteSupport<T> writeSupport
    ) {
        return new ParquetWriterFactory<>(new DeltaParquetBuilder<T>(conf, writeSupport));
    }


    public static class DeltaParquetBuilder<T> implements ParquetBuilder<T> {

        private final SerializableConfiguration configuration;
        private final WriteSupport<T> writeSupport;

        public DeltaParquetBuilder(Configuration conf,
                                   WriteSupport<T> writeSupport) {
            this.configuration = new SerializableConfiguration(conf);
            this.writeSupport = writeSupport;
        }

        @Override
        public ParquetWriter<T> createWriter(OutputFile out) throws IOException {
            Configuration conf = configuration.conf();
            return new DeltaParquetWriterBuilder<>(out, this.writeSupport)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withPageSize(getPageSize(conf))
                    .withDictionaryPageSize(getDictionaryPageSize(conf))
                    .withMaxPaddingSize(
                            conf.getInt(MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT))
                    .withDictionaryEncoding(getEnableDictionary(conf))
                    .withValidation(getValidation(conf))
                    .withWriterVersion(getWriterVersion(conf))
                    .withConf(conf)
                    .build();
        }
    }
}