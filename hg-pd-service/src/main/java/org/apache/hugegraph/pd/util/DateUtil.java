package org.apache.hugegraph.pd.util;

import com.baidu.hugegraph.pd.common.PDException;
import com.baidu.hugegraph.pd.grpc.Pdpb;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.Date;

/**
 * @author zhangyingjie
 * @date 2022/3/23
 **/
public class DateUtil {
    private static String DATE = "yyyy-MM-dd";
    private static String DATETIME = "yyyy-MM-dd HH:mm:ss";
    private static String DATETIME_MM = "yyyy-MM-dd HH:mm";
    private static String DATETIME_SSS = "yyyy-MM-dd HH:mm:ss.SSS";
    private static String TIME = "HH:mm";
    private static String TIME_SS = "HH:mm:ss";
    private static String SYS_DATE = "yyyy/MM/dd";
    private static String SYS_DATETIME = "yyyy/MM/dd HH:mm:ss";
    private static String SYS_DATETIME_MM = "yyyy/MM/dd HH:mm";
    private static String SYS_DATETIME_SSS = "yyyy/MM/dd HH:mm:ss.SSS";
    private static String NONE_DATE = "yyyyMMdd";
    private static String NONE_DATETIME = "yyyyMMddHHmmss";
    private static String NONE_DATETIME_MM = "yyyyMMddHHmm";
    private static String NONE_DATETIME_SSS = "yyyyMMddHHmmssSSS";
    private static String[] PATTERNS =new String[]{
            DATE,
            DATETIME,
            DATETIME_MM,
            DATETIME_SSS,
            TIME,
            TIME_SS,
            SYS_DATE,
            SYS_DATETIME,
            SYS_DATETIME_MM,
            SYS_DATETIME_SSS,
            NONE_DATE,
            NONE_DATETIME,
            NONE_DATETIME_MM,
            NONE_DATETIME_SSS
    };

    public static String[] getDefaultPattern(){
        return PATTERNS;
    }

    public static Date getDate(String date) throws PDException {
        try {
            return DateUtils.parseDate(date,getDefaultPattern());
        } catch (ParseException e) {
            throw new PDException(Pdpb.ErrorType.UNKNOWN_VALUE, e.getMessage());
        }
    }

}
