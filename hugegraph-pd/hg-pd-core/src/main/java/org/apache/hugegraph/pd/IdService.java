/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.pd;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.meta.IdMetaStore;
import org.apache.hugegraph.pd.meta.MetadataFactory;

public class IdService {

    private final IdMetaStore meta;
    private PDConfig pdConfig;

    public IdService(PDConfig config) {
        this.pdConfig = config;
        meta = MetadataFactory.newHugeServerMeta(config);
    }

    public PDConfig getPdConfig() {
        return pdConfig;
    }

    public void setPdConfig(PDConfig pdConfig) {
        this.pdConfig = pdConfig;
    }

    public long getId(String key, int delta) throws PDException {
        return meta.getId(key, delta);
    }

    public void resetId(String key) throws PDException {
        meta.resetId(key);
    }

    /**
     * 获取自增循环不重复id, 达到上限后从0开始自增.自动跳过正在使用的cid
     *
     * @param key
     * @param max
     * @return
     * @throws PDException
     */
    public long getCId(String key, long max) throws PDException {
        return meta.getCId(key, max);
    }

    public long getCId(String key, String name, long max) throws PDException {
        return meta.getCId(key, name, max);
    }

    /**
     * 删除一个自增循环id
     *
     * @param key
     * @param cid
     * @return
     * @throws PDException
     */
    public long delCId(String key, long cid) throws PDException {
        return meta.delCId(key, cid);
    }

    public long delCIdDelay(String key, String name, long cid) throws PDException {
        return meta.delCIdDelay(key, name, cid);
    }
}
