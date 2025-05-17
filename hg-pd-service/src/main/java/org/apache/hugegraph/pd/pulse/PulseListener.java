package org.apache.hugegraph.pd.pulse;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/9
 */
public interface PulseListener <T> {
    /**
     * Invoked on new notice.
     *
     * @param notice the notice.
     */
    void onNext(T notice) throws Exception;

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