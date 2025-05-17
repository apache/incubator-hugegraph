package org.apache.hugegraph.pd.util.grpc;

import io.grpc.Grpc;
import io.grpc.ServerCall;
import io.grpc.stub.StreamObserver;

import java.lang.reflect.Field;

public class StreamObserverUtil {

    static Object fieldLock = new Object();
    static Field callField;

    public static String getRemoteIP(StreamObserver observer) {
        String ip = "";
        try {
            if (callField == null) {
                synchronized (fieldLock) {
                    callField = observer.getClass().getDeclaredField("call");
                    callField.setAccessible(true);
                }
            }
            ServerCall call = (ServerCall) callField.get(observer);
            if (call != null) {
                ip = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR).toString();
            }
        } catch (Exception e) {

        }
        return ip;
    }
}
