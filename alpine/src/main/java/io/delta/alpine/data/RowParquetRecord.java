package io.delta.alpine.data;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

public interface RowParquetRecord {
//    int getInt(String fieldName);
//    long getLong(String fieldName);
//    byte getByte(String fieldName);
//    short getShort(String fieldName);
//    boolean getBoolean(String fieldName);
//    float getFloat(String fieldName);
//    double getDouble(String fieldName);
//    String getString(String fieldName);
//    Timestamp getTimestamp(String fieldName);
//    Date getDate(String fieldName);
////    byte[] getByteArray(String fieldName); // getBinary ??
//    RowParquetRecord getRecord(String fieldName);
//    <T> T[] getArray(String fieldName);
//    <T> T[] getArray(String fieldName);
    <T> T getAs(String fieldName);
//    <K, V> Map<K, V> getMap(String fieldName);
}
