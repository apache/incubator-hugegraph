package org.apache.hugegraph.pd.core.meta;

import org.apache.hugegraph.pd.meta.MetadataKeyHelper;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class MetadataKeyHelperTest {

    @Test
    public void testMoveTaskKey(){
        var key = MetadataKeyHelper.getMoveTaskKey("foo", 0, 1);
        assertTrue(Arrays.equals(key, "TASK_MOVE/foo/0/1".getBytes()));
        var key2 = MetadataKeyHelper.getMoveTaskPrefix("foo");
        assertTrue(Arrays.equals(key2, "TASK_MOVE/foo".getBytes()));
    }
}
