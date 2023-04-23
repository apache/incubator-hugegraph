package common;

import com.baidu.hugegraph.store.buffer.KVByteBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KVByteBufferTest {

    @Test
    public void testOps(){
        KVByteBuffer buffer1 = new KVByteBuffer(10);
        buffer1.put((byte) 10);
        // just put a byte
        assertEquals(1, buffer1.position());
        // 9 left
        assertTrue(buffer1.hasRemaining());
        buffer1.clear();
        assertEquals(10, buffer1.get());

        buffer1.clear();
        buffer1.putInt(10);
        buffer1.clear();
        assertEquals(10, buffer1.getInt());

        buffer1.flip();
        // just write to a int
        assertEquals(4, buffer1.getBuffer().limit());

        byte[] bytes = new byte[]{10, 20, 30};
        KVByteBuffer buffer2 = new KVByteBuffer(bytes);
        assertTrue(Arrays.equals(buffer2.array(), bytes));


        ByteBuffer bb = ByteBuffer.allocate(10);
        KVByteBuffer buffer3 = new KVByteBuffer(bb);
        buffer3.put(bytes);
        buffer3.clear();
        assertTrue(Arrays.equals(buffer3.getBytes(), bytes));

        // int (4) + byte(3)
        assertEquals(7, buffer3.getBuffer().position());

        ByteBuffer bb2 = buffer3.copyBuffer();
        assertEquals(7, bb2.capacity());
    }
}
