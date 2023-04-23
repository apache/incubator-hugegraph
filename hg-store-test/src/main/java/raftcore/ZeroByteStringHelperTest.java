package raftcore;

import com.google.protobuf.ZeroByteStringHelper;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ZeroByteStringHelperTest {

    private static final String STR = "hello word!";

    @Test
    public void testWrap(){
        byte[] b1 = new byte[]{10, 20, 30};
        byte[] b2 = new byte[]{40, 50};

        var h1 = ZeroByteStringHelper.wrap(b1);
        var h2 = ZeroByteStringHelper.wrap(b2, 0, b2.length);

        ByteBuffer buffer = ByteBuffer.allocate(5);
        buffer.put(b1);
        buffer.put(b2);
        var h3 = ZeroByteStringHelper.wrap(buffer);
        assertEquals(h3.isEmpty(), true);
        var h4 = ZeroByteStringHelper.concatenate(h1, h2);
        assertTrue(Arrays.equals(ZeroByteStringHelper.getByteArray(h4), buffer.array()));
    }

    @Test
    public void testConcatenate(){
        byte[] b1 = new byte[]{10, 20, 30};
        byte[] b2 = new byte[]{40, 50};
        ByteBuffer buffer1 = ByteBuffer.allocate(5);
        buffer1.put(b1);

        ByteBuffer buffer2 = ByteBuffer.allocate(5);
        buffer1.put(b2);

        var array = new ArrayList<ByteBuffer>();
        array.add(buffer1);
        array.add(buffer2);

        var bs = ZeroByteStringHelper.concatenate(array);
        assertEquals(bs.toByteArray().length, 5);
    }
}
