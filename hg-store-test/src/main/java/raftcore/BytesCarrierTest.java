package raftcore;

import com.google.protobuf.BytesCarrier;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BytesCarrierTest {

    @Test
    public void testWrite() throws IOException {
        byte[] bytes = new byte[]{10, 20,30};
        BytesCarrier carrier = new BytesCarrier();

        // not valid
        carrier.write((byte) 1);
        assertNull(carrier.getValue());
        assertFalse(carrier.isValid());

        // not valid
        ByteBuffer buffer = ByteBuffer.allocate(10);
        carrier.write(buffer);
        assertNull(carrier.getValue());

        // not valid
        carrier.writeLazy(buffer);
        assertNull(carrier.getValue());

        // ok, write done
        carrier.write(bytes, 0, bytes.length);
        assertNotNull(carrier.getValue());
        assertTrue(carrier.isValid());

        // has data
        carrier.writeLazy(bytes, 0, bytes.length);
        assertFalse(carrier.isValid());
    }
}
