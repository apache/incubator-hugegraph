package core.raft;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.store.raft.RaftOperation;
import com.google.protobuf.GeneratedMessageV3;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class RaftOperationTest {

    private RaftOperation raftOperationUnderTest;

    @Before
    public void setUp() {
        raftOperationUnderTest = new RaftOperation();
    }



    @Test
    public void testCreate1() {
        // Run the test
        final RaftOperation result = RaftOperation.create((byte) 0b0);
        assertEquals(null, result.getReq());
        assertEquals((byte) 0b0, result.getOp());
    }

    @Test
    public void testCreate2() {
        // Run the test
        final RaftOperation result = RaftOperation.create((byte) 0b0, "content".getBytes(), "req");
        assertArrayEquals("content".getBytes(), result.getValues());
        assertEquals("req", result.getReq());
        assertEquals((byte) 0b0, result.getOp());
    }

    @Test
    public void testCreate3() {
        // Run the test
        final RaftOperation result = RaftOperation.create((byte) 0b0, "req");
        assertEquals("req", result.getReq());
        assertEquals((byte) 0b0, result.getOp());
    }

    @Test
    public void testCreate4() throws Exception {
        // Setup
        final GeneratedMessageV3 req = Metapb.Graph.newBuilder().setGraphName("name").build();

        // Run the test
        final RaftOperation result = RaftOperation.create((byte) 0b0, req);
        assertEquals((byte) 0b0, result.getOp());

    }
}
