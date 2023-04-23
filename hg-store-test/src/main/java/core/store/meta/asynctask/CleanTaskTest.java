package core.store.meta.asynctask;

import com.baidu.hugegraph.store.meta.asynctask.AbstractAsyncTask;
import com.baidu.hugegraph.store.meta.asynctask.AsyncTask;
import com.baidu.hugegraph.store.meta.asynctask.AsyncTaskState;
import com.baidu.hugegraph.store.meta.asynctask.CleanTask;
import core.StoreEngineTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CleanTaskTest extends StoreEngineTestBase {

    @Test
    public void testSerialize(){
        CleanTask task = new CleanTask(0, "graph0", AsyncTaskState.SUCCESS, null);
        byte[] bytes = task.toBytes();

        AsyncTask task2 = AbstractAsyncTask.fromBytes(bytes);
        assertEquals(CleanTask.class, task2.getClass());
        System.out.println(task2);

        createPartitionEngine(0);

        CleanTask task3 = new CleanTask(0, "graph0", AsyncTaskState.START, null);
        CleanTask task4 = new CleanTask(0, "graph0", AsyncTaskState.FAILED, null);
        task3.handleTask();
        task4.handleTask();
    }

}
