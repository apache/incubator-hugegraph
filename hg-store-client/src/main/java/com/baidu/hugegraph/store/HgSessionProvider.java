package com.baidu.hugegraph.store;

import javax.annotation.concurrent.ThreadSafe;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/12
 */
@ThreadSafe
public interface HgSessionProvider {
    HgStoreSession createSession(String graphName);
}
