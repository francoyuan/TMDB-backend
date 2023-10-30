package edu.whu.tmdb.query.utils;/*
 * className:MapToDataType
 * Package:edu.whu.tmdb.query.operations.utils
 * Description:
 * @Author: xyl
 * @Create:2023/10/14 - 14:21
 * @Version:v1
 */

import net.sf.jsqlparser.expression.*;
import org.apache.zookeeper.Op;
import org.checkerframework.checker.nullness.Opt;
import org.checkerframework.checker.units.qual.A;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Optional;

public class MapToDataType {
    public static Optional<Object> mapToDataType(Object data) {
        switch (data.getClass().getSimpleName()){
            case "LongValue":
                return Optional.of(((LongValue)data).getValue());
            case "SignedExpression":
                return Optional.of(Long.parseLong(((SignedExpression)data).toString()));
            case "ArrayConstructor":
                return arrayHandle(((ArrayConstructor)data));
            case "StringValue":
                return Optional.of(((StringValue)data).getValue());
            default:
                return Optional.of(((String)data.toString()));
        }
    }

    public static Optional<Object> arrayHandle(ArrayConstructor arrayConstructor){
        int size = arrayConstructor.getExpressions().size();
        double[] res=new double[size];
        ArrayList<Double> arrayList=new ArrayList<>();
        for (int i = 0; i < size; i++) {
            res[i]=Double.parseDouble(arrayConstructor.getExpressions().get(i).toString());
        }
        return Optional.of(res);
    }
}
