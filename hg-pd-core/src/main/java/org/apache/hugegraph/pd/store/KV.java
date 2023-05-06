package org.apache.hugegraph.pd.store;


public class KV {
    private byte[] key;
    private byte[] value;

    public KV(byte[] key, byte[] value){
        this.key = key;
        this.value = value;
    }
    public void setKey(byte[] key) {
        this.key = key;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }
}
