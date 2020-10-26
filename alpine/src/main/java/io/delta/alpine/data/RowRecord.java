package io.delta.alpine.data;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import io.delta.alpine.types.StructType;
import io.delta.alpine.types.StructField;

/**
 * Represents one row of data containing a non-empty collection of {@code fieldName - value} pairs.
 * <p>
 * Allows retrieval of values only through {@code fieldName} lookup. For example: {@code int x = getInt("int_field")}.
 * <p>
 * It is valid to retrieve a value that is {@code null} only if the schema field is nullable.
 * <p>
 * Immutable and <b>NOT</b> thread safe.
 *
 * @see StructType StructType
 * @see StructField StructField
 */
public interface RowRecord {

    /**
     * @return the schema for this RowRecord
     */
    StructType getSchema();

    /**
     * @return the number of elements in this RowRecord
     */
    int getLength();

    /**
     * Retrieves value from data record and returns the value as a primitive int.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as a primitive int. {@code null} only if
     *         {@code null} value read and field is nullable.
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if field is not nullable and {@code null} data value read
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

    /**
     *
     * @param fieldName
     * @param <T> element type
     * @return
     */
    <T> List<T> getList(String fieldName);

    /**
     *
     * @param fieldName
     * @param <K> key type
     * @param <V> value type
     * @return
     */
    <K, V> Map<K, V> getMap(String fieldName);
}
