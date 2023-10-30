package edu.whu.tmdb.query.utils;/*
 * className:KryoSerialization
 * Package:edu.whu.tmdb.query.operations.utils
 * Description:
 * @Author: xyl
 * @Create:2023/10/24 - 16:02
 * @Version:v1
 */

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.whu.tmdb.storage.memory.Tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Base64;
import java.util.HashMap;

public class KryoSerialization {

    private static final Kryo kryo = new Kryo();
    static {  // 使用静态初始化块进行注册
        kryo.setRegistrationRequired(false);
    }


    public static byte[] serialize(Object object) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, object);
        }
        return baos.toByteArray();
    }

    public static Object deserialize(byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);

        try (Input input = new Input(bais)) {
            return  kryo.readClassAndObject(input);
        }
    }

    public static String serializeToString(Object object) {

        byte[] bytes = serialize(object);
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static Object deserializeFromString(String data) {

        byte[] bytes = Base64.getDecoder().decode(data);
        return  deserialize(bytes);
    }
}

