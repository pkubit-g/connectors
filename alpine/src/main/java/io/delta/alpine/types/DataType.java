package io.delta.alpine.types;

import java.util.Locale;

public abstract class DataType {
    public String getTypeName() {
        String tmp = this.getClass().getSimpleName();
        tmp = stripSuffix(tmp, "$");
        tmp = stripSuffix(tmp, "Type");
        tmp = stripSuffix(tmp, "UDT");
        return tmp.toLowerCase(Locale.ROOT);
    }

    /** Readable string representation for the type. */
    public String getSimpleString() {
        return getTypeName();
    }

    /** String representation for the type saved in external catalogs. */
    public String getCatalogString() {
        return getSimpleString();
    }

    public static void buildFormattedString(
            DataType dataType,
            String prefix,
            StringBuilder builder) {
        if (dataType instanceof ArrayType) ((ArrayType) dataType).buildFormattedString(prefix, builder);
        if (dataType instanceof StructType) ((StructType) dataType).buildFormattedString(prefix, builder);
        if (dataType instanceof MapType) ((MapType) dataType).buildFormattedString(prefix, builder);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataType that = (DataType) o;
        return getTypeName().equals(that.getTypeName());
    }

    private String stripSuffix(String orig, String suffix) {
        if (null != orig && orig.endsWith(suffix)) {
            return orig.substring(0, orig.length() - suffix.length());
        }
        return orig;
    }
}
