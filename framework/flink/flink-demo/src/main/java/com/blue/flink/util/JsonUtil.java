package com.blue.flink.util;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {
    public static ObjectMapper objectMapper = new ObjectMapper();

    public static String toJsonString(Object e) throws Exception {
        return objectMapper.writeValueAsString(e);
    }

    public static <T> T fromJsonString(String json,Class<T> valueType) throws Exception {
        return objectMapper.readValue(json, valueType);
    }
}
