package edu.whu.tmdb.query.enums;/*
 * className:DeputyType
 * Package:edu.whu.tmdb.query.operations.enums
 * Description:
 * @Author: xyl
 * @Create:2023/10/9 - 16:03
 * @Version:v1
 */

public enum DeputyType {
    SELECT_DEPUTY("selectdeputy", 0),
    JOIN_DEPUTY("joindeputy", 1),
    UNION_DEPUTY("uniondeputy", 2),
    GROUPBY_DEPUTY("groupbydeputy", 3);

    private final String value;
    private final int intValue;

    DeputyType(String value, int intValue) {
        this.value = value;
        this.intValue = intValue;
    }

    public String getValue() {
        return value;
    }

    public int getIntValue() {
        return intValue;
    }

    public static DeputyType fromValue(String value) {
        for (DeputyType type : DeputyType.values()) {
            if (type.getValue().equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Invalid value: " + value);
    }
}



