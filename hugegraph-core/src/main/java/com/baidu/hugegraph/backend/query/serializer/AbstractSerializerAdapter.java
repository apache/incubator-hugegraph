package com.baidu.hugegraph.backend.query.serializer;

import com.baidu.hugegraph.backend.BackendException;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.Map;

public abstract class AbstractSerializerAdapter<T> implements JsonSerializer<T>,
                                                              JsonDeserializer<T> {

    //Note: By overriding the method to get the mapping
    public abstract Map<String,Type> validType();
    @Override
    public T deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
        JsonObject object = json.getAsJsonObject();
        String type = object.get("cls").getAsString();
        JsonElement element = object.get("el");
        try {
            return context.deserialize(element, validType().get(type));
        } catch (Exception e) {
            throw new BackendException("Unknown element type: " + type, e);
        }
    }

    /*
    * Note: Currently, only the first character of the class name is taken as the key
    *       to reduce serialization results
    * */
    @Override
    public JsonElement serialize(T src, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject result = new JsonObject();
        Class clazz = src.getClass();
        result.add("cls", new JsonPrimitive(clazz.getSimpleName().substring(0, 1).toUpperCase()));
        result.add("el", context.serialize(src, clazz));
        return result;
    }

}
