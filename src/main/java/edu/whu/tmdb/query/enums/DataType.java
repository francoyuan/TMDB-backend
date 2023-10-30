package edu.whu.tmdb.query.enums;/*
 * className:DataType
 * Package:edu.whu.tmdb.query.operations.utils
 * Description:
 * @Author: xyl
 * @Create:2023/10/14 - 14:12
 * @Version:v1
 */

import java.util.Locale;
import java.util.Optional;

public enum DataType {
    // ... [其他已有的枚举值]

    VARCHAR("VARCHAR"),
    CHAR("CHAR"),
    TEXT("TEXT"),
    INT("INT"),
    BIGINT("BIGINT"),
    FLOAT("FLOAT"),
    DOUBLE("DOUBLE"),
    DECIMAL("DECIMAL"),
    DATE("DATE"),
    DATETIME("DATETIME"),
    TIMESTAMP("TIMESTAMP"),
    TIME("TIME"),
    YEAR("YEAR"),
    BLOB("BLOB"),
    MEDIUMBLOB("MEDIUMBLOB"),
    LONGBLOB("LONGBLOB"),
    TINYBLOB("TINYBLOB"),
    BOOLEAN("BOOLEAN"),
    ENUM("ENUM"),
    SET("SET"),
    JSON("JSON"),
    BINARY("BINARY"),
    VARBINARY("VARBINARY"),
    TINYINT("TINYINT"),
    SMALLINT("SMALLINT"),
    MEDIUMINT("MEDIUMINT"),
    // Adding array types for PostgreSQL
    TEXT_ARRAY("TEXT[]"),
    INT_ARRAY("INT[]"),
    DOUBLE_ARRAY("DOUBLE[]"),
    VARCHAR_ARRAY("VARCHAR[]"),
    // ... [可以继续添加其他数组类型]
    ;

    private final String type;

    DataType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return type;
    }

    public static Optional<DataType> mapToDataType(String typeStr) {
        for (DataType type : DataType.values()) {
            if (type.toString().equalsIgnoreCase(typeStr)) {
                return Optional.of(type);
            }
        }
        return Optional.of(DataType.CHAR);
    }

    public static Optional<DataType> jspToDataType(String typeStr) {
        switch (typeStr.toLowerCase(Locale.ROOT)){
            case "stringvalue": return Optional.of(DataType.CHAR);
            case "signedexpression": return Optional.of(DataType.DOUBLE);
            case "longvalue": return Optional.of(DataType.INT);
            case "doublevalue": return Optional.of(DataType.DOUBLE);
            case "arrayconstructor": return Optional.of(DataType.DOUBLE_ARRAY);
            default: return Optional.of(DataType.CHAR);
        }
    }

}
