package org.apache.hugegraph.pd.pulse;

/**
 * @author lynn.bond@hotmail.com created on 2022/2/13
 */
public interface PulseServerNotice<T> {
    /**
     * @throws RuntimeException when failed to send ack-message to pd-server
     */
    void ack();

    long getNoticeId();

    /**
     * Return a response object of gRPC stream.
     * @return
     */
    T getContent();

}
