package io.delta.standalone.data;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public interface RowRecordBuilder {
    RowRecordBuilder put(String fieldName, int value);
    RowRecordBuilder put(String fieldName, long value);
    RowRecordBuilder put(String fieldName, byte value);
    RowRecordBuilder put(String fieldName, short value);
    RowRecordBuilder put(String fieldName, boolean value);
    RowRecordBuilder put(String fieldName, float value);
    RowRecordBuilder put(String fieldName, double value);
    RowRecordBuilder put(String fieldName, String value);
    RowRecordBuilder put(String fieldName, byte[] value);
    RowRecordBuilder put(String fieldName, BigDecimal value);
    RowRecordBuilder put(String fieldName, Timestamp value);
    RowRecordBuilder put(String fieldName, Date value);
    RowRecordBuilder put(String fieldName, RowRecord value); // nested Struct
    <T> RowRecordBuilder put(String fieldName, List<T> value);
    <K, V> RowRecordBuilder put (String fieldName, Map<K, V> value);

    RowRecord build();
}
