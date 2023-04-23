package com.baidu.hugegraph.store.cmd;

public class GetStoreInfoRequest extends HgCmdBase.BaseRequest{
    @Override
    public byte magic() {
        return HgCmdBase.GET_STORE_INFO;
    }
}
