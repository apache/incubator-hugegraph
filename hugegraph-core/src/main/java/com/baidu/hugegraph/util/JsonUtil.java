package com.baidu.hugegraph.util;

import com.google.gson.Gson;

/**
 * Created by liningrui on 2017/4/28.
 */
public class JsonUtil {

    private static Gson gson = new Gson();

    public static String toJson(Object object) {
        return gson.toJson(object);
    }

    public static <T> T fromJson(String json, Class<T> clazz) {
        return gson.fromJson(json, clazz);
    }

}
