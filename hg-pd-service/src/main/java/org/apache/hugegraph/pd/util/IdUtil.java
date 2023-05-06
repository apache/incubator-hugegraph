package org.apache.hugegraph.pd.util;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com on 2022/2/8
 */
@Slf4j
public final class IdUtil {
    private final static byte[] lock = new byte[0];

    public static String createMillisStr(){
        return String.valueOf(createMillisId());
    }

    /**
     * Create millisecond style ID;
     * @return
     */
    public static Long createMillisId() {
        synchronized (lock) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                log.error("Failed to sleep", e);
            }

            return System.currentTimeMillis();
        }

    }
}
