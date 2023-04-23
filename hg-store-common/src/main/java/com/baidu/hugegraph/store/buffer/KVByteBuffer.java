package com.baidu.hugegraph.store.buffer;

import java.nio.ByteBuffer;

public class KVByteBuffer {
    ByteBuffer buffer;
    public KVByteBuffer(int capacity){
        buffer = ByteBuffer.allocate(capacity);
    }

    public KVByteBuffer(byte[] buffer){
        this.buffer = ByteBuffer.wrap(buffer);
    }

    public KVByteBuffer(ByteBuffer buffer){
        this.buffer = buffer;
    }
    public void clear(){
        this.buffer.clear();
    }

    public KVByteBuffer flip(){
        buffer.flip();
        return this;
    }
    public ByteBuffer getBuffer(){ return buffer;}

    public ByteBuffer copyBuffer(){
        byte[] buf = new byte[buffer.position()];
        System.arraycopy(buffer.array(), 0, buf, 0, buffer.position());
        return ByteBuffer.wrap(buf);
    }
    public void put(byte data){
        buffer.put(data);
    }

    public void put(byte[] data){
        if ( data != null ) {
            buffer.putInt(data.length);
            buffer.put(data);
        }
    }

    public byte[] getBytes(){
        int len = buffer.getInt();
        byte[] data = new byte[len];
        buffer.get(data);
        return data;
    }

    public byte get(){
        return buffer.get();
    }
    public void putInt(int data){
        buffer.putInt(data);
    }

    public int getInt(){
        return buffer.getInt();
    }
    public byte[] array(){
        return this.buffer.array();
    }

    public int position(){ return this.buffer.position();}

    public final boolean hasRemaining(){ return this.buffer.hasRemaining();}
}
