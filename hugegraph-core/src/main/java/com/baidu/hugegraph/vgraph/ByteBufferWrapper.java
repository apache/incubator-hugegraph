package com.baidu.hugegraph.vgraph;

import java.nio.ByteBuffer;

public class ByteBufferWrapper {

    private int offset;
    private ByteBuffer byteBuffer;

    public ByteBufferWrapper(int offset, ByteBuffer byteBuffer) {
        this.offset = offset;
        this.byteBuffer = byteBuffer;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public int getOffset() {
        return offset;
    }

    public byte[] getBytes() {
        int length = this.byteBuffer.position() - this.offset;
        byte[] result = new byte[length];
        ByteBuffer bufferWrapped = ByteBuffer.wrap(this.byteBuffer.array(), this.offset, length);
        bufferWrapped.get(result);
        return result;
    }
}
