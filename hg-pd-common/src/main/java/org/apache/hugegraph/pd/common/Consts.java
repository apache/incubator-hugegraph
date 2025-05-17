package org.apache.hugegraph.pd.common;

import io.grpc.Metadata;

/**
 * @author zhangyingjie
 * @date 2023/4/25
 **/
public class Consts {
    public static final Metadata.Key<String> CREDENTIAL_KEY = Metadata.Key.of("credential",
                                                                              Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> TOKEN_KEY = Metadata.Key.of("Pd-Token",
                                                                         Metadata.ASCII_STRING_MARSHALLER);

    public static final Metadata.Key<String> LEADER_KEY = Metadata.Key.of("leader",
                                                                          Metadata.ASCII_STRING_MARSHALLER);

    public static final int DEFAULT_STORE_GROUP_ID = 0;
    /**
     * store group 的partition间隔
     */
    public static final int PARTITION_GAP = 1000;
}
