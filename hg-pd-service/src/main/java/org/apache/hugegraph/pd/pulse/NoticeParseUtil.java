package org.apache.hugegraph.pd.pulse;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Parser;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author lynn.bond@hotmail.com on 2024/2/26
 */
@Slf4j
class NoticeParseUtil {
    private static final Map<String, Parser> PARSER_HOLDER = new ConcurrentHashMap<>();

    public static GeneratedMessageV3 parseNotice(ByteString instanceData, String className) {
        Class clazz = null;

        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            log.error("Failed to retrieve the Class of notice with class-name: "
                    + className + ", caused by error: ", e);
            return null;
        }

        return deserializeNotice(instanceData, clazz);
    }

    public static <T extends GeneratedMessageV3> T deserializeNotice(ByteString data, Class<T> noticeClass) {
        Parser<T> parser = getNoticeParser(noticeClass);
        if (parser == null) {
            return null;
        }

        T message = null;

        try {
            message = (T) parser.parseFrom(data);
        } catch (Exception e) {
            log.error("Failed to deserialize notice with class-name: " + noticeClass.getTypeName(), e);
            return null;
        }

        return message;
    }

    public static <T> Parser<T> getNoticeParser(Class<T> noticeClass) {
        Parser<T> parser = (Parser<T>) PARSER_HOLDER.get(noticeClass.getTypeName());

        if (parser != null) {
            return parser;
        }

        try {
            Method parseFromMethod = noticeClass.getMethod("parser");
            parser = (Parser<T>) parseFromMethod.invoke(null);
        } catch (Exception e) {
            log.error("Failed to fetch the Parser of notice, class name: " + noticeClass.getTypeName(), e);
            return null;
        }

        if (parser == null) {
            log.error("There is no Parser for the notice with the class name: " + noticeClass.getTypeName());
            return null;
        }

        PARSER_HOLDER.put(noticeClass.getTypeName(), parser);

        return parser;
    }
}
