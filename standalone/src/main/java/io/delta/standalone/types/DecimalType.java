package io.delta.standalone.types;

/**
 * The data type representing {@code java.math.BigDecimal} values.
 * A Decimal that must have fixed precision (the maximum number of digits) and scale (the number
 * of digits on right side of dot).
 *
 * The precision can be up to 38, scale can also be up to 38 (less or equal to precision).
 *
 * The default precision and scale is (10, 0).
 */
public final class DecimalType extends DataType {
    public static final DecimalType USER_DEFAULT = new DecimalType(10, 0);

    private final int precision;
    private final int scale;

    public DecimalType(int precision, int scale) {
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * @return the maximum number of digits of the decimal
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * @return the number of digits on the right side of the decimal point (dot)
     */
    public int getScale() {
        return scale;
    }
}
