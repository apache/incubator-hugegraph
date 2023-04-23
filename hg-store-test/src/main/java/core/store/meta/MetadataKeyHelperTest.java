package core.store.meta;

import com.baidu.hugegraph.store.meta.MetadataKeyHelper;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class MetadataKeyHelperTest {

    @Test
    public void testKey(){
        assertTrue(Arrays.equals("HUGEGRAPH/TASK/".getBytes(), MetadataKeyHelper.getTaskPrefix()));
        assertTrue(Arrays.equals("HUGEGRAPH/TASK/0/".getBytes(), MetadataKeyHelper.getTaskPrefix(0)));
        assertTrue(Arrays.equals("HUGEGRAPH/TASK_DONE/0000000000000000".getBytes(),
                MetadataKeyHelper.getDoneTaskKey(0)));
    }
}
