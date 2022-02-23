package com.baidu.hugegraph.vgraph;

import com.baidu.hugegraph.backend.serializer.BinarySerializer;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeProperty;
import java.nio.ByteBuffer;
import java.util.Collection;

public abstract class VirtualElement {

    protected static final BinarySerializer SERIALIZER = new BinarySerializer();

    protected byte status;
    protected long expiredTime;
    protected ByteBufferWrapper propertyBuf;


    protected VirtualElement(HugeElement element,
                             byte status) {
        assert element != null;
        this.expiredTime = element.expiredTime();
        this.status = status;
        if (element.hasProperties()) {
            setProperties(element.getProperties().values());
        }
    }

    protected VirtualElement() { }

    protected BytesBuffer getPropertiesBufForRead() {
        assert propertyBuf != null;
        ByteBuffer byteBuffer = propertyBuf.getByteBuffer();
        return BytesBuffer.wrap(byteBuffer.array(), propertyBuf.getOffset(),
                byteBuffer.position() - propertyBuf.getOffset());
    }

    public void fillProperties(HugeElement owner) {
        if (propertyBuf != null) {
            SERIALIZER.parseProperties(getPropertiesBufForRead(), owner);
        }
    }

    public void setProperties(Collection<HugeProperty<?>> properties) {
        if (properties != null && !properties.isEmpty()) {
            BytesBuffer buffer = new BytesBuffer();
            SERIALIZER.formatProperties(properties, buffer);
            ByteBuffer innerBuffer = buffer.asByteBuffer();
            propertyBuf = new ByteBufferWrapper(innerBuffer.arrayOffset(), innerBuffer);
        } else {
            propertyBuf = null;
        }
    }

    public void copyProperties(VirtualElement element) {
        propertyBuf = element.propertyBuf;
    }

    public byte getStatus() {
        return status;
    }
}
