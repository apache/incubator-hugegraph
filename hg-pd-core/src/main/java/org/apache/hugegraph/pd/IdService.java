package org.apache.hugegraph.pd;

import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.meta.IdMetaStore;
import org.apache.hugegraph.pd.meta.MetadataFactory;

import com.baidu.hugegraph.pd.common.PDException;

public class IdService {

    public PDConfig getPdConfig() {
        return pdConfig;
    }

    public void setPdConfig(PDConfig pdConfig) {
        this.pdConfig = pdConfig;
    }

    private PDConfig pdConfig;
    private IdMetaStore meta;

    public IdService(PDConfig config) {
        this.pdConfig = config;
        meta = MetadataFactory.newHugeServerMeta(config);
    }

    public long getId(String key, int delta) throws PDException {
        return meta.getId(key, delta);
    }

    public void resetId(String key) throws PDException {
        meta.resetId(key);
    }

    /**
     * 获取自增循环不重复id, 达到上限后从0开始自增.自动跳过正在使用的cid
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
