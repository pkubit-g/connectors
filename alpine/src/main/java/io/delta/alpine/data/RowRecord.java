package io.delta.alpine.data;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import io.delta.alpine.types.StructType;
import io.delta.alpine.types.StructField;

/**
 * Represents one row of data containing a non-empty collection of fieldName - value pairs.
 *
 * Allows retrieval of values only through {@code fieldName} lookup, e.g. {@code int x = getInt("int_field")}.
 *
 * It is valid to retrieve a value that is {@code null} only if the {@link StructType schema}
 * {@link StructField field} is nullable.
 *
 * Immutable and <b>NOT</b> thread safe.
 */
public interface RowRecord {
    /** Schema for the RowRecord */
    StructType getSchema();

    /** Number of elements in the RowRecord. */
    int getLength();

    /**
     * Returns the value for field `fieldName` as a primitive int.
     *
     * @throws IllegalArgumentException when `fieldName` does not exist.
     * @throws NullPointerException when null value found for field declared to be not nullable.
     * @throws ClassCastException when data type does not match.
     */
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
    RowRecord getRecord(String fieldName);
    <T> List<T> getList(String fieldName);
    <K, V> Map<K, V> getMap(String fieldName);
}
