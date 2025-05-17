package org.apache.hugegraph.pd.pulse;


public interface PulseListener<T> {
    /**
     * Invoked on new events.
     *
     * @param response the response.
     */
    @Deprecated
    default void onNext(T response){};

    /**
     * Invoked on new events.
     * @param notice a wrapper of response
     */
    default void onNotice(PulseServerNotice<T> notice){
        notice.ack();
    }

    /**
     * Invoked on errors.
     *
     * @param throwable the error.
     */
    void onError(Throwable throwable);

    /**
     * Invoked on completion.
     */
    void onCompleted();

}
