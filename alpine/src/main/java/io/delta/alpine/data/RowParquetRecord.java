package io.delta.alpine.data;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public interface RowParquetRecord {
    int getInt(String fieldName);
    long getLong(String fieldName);
    byte getByte(String fieldName);
    short getShort(String fieldName);
    boolean getBoolean(String fieldName);
    float getFloat(String fieldName);
    double getDouble(String fieldName);
    String getString(String fieldName);
    byte[] getBinary(String fieldName);
    BigDecimal getBigDecimal(String fieldName);
    Timestamp getTimestamp(String fieldName);
    Date getDate(String fieldName);
    RowParquetRecord getRecord(String fieldName);
    <T> List<T> getList(String fieldName);
    <K, V> Map<K, V> getMap(String fieldName);
}
