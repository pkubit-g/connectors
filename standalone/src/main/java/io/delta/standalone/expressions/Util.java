package io.delta.standalone.expressions;

import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.IntegerType;

import java.util.Comparator;

public class Util {

    /**
     * Returns the result of the comparison between `o1` and `o2`, based on their DataType.
     */
    public static int compare(DataType compareType, Object o1, Object o2) {
        if (compareType instanceof BooleanType) {
            return Comparator.<Boolean>naturalOrder().compare((Boolean) o1, (Boolean) o2);
        }
        if (compareType instanceof IntegerType) {
            return Comparator.<Integer>naturalOrder().compare((Integer) o1, (Integer) o2);
        }

        throw new RuntimeException("Util::compare > unrecognized compareType: " + compareType.toString());
    }
}
