package io.delta.alpine.data;

import java.sql.Date;
import java.sql.Timestamp;

public interface RowParquetRecord {
    int getInt(String fieldName);
    long getLong(String fieldName);
    byte getByte(String fieldName);
    short getShort(String fieldName);
    boolean getBoolean(String fieldName);
    float getFloat(String fieldName);
    double getDouble(String fieldName);
    String getString(String fieldName);
    Timestamp getTimestamp(String fieldName);
    Date getDate(String fieldName);
//    byte[] getByteArray(String fieldName); // getBinary ??
    RowParquetRecord getRecord(String fieldName);
    <T> T[] getArray(String fieldName);
    <T> T get(String fieldName);
}
