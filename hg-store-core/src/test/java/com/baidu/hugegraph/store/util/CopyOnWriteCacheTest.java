package com.baidu.hugegraph.store.util;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
// import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class CopyOnWriteCacheTest {

    // @Test
    public void testCache() throws InterruptedException {
        Map<String, String> cache = new CopyOnWriteCache<>(1000);
        cache.put("1", "1");
        Thread.sleep(2000);
        Asserts.isTrue(!cache.containsKey("1"), "cache do not clear");
    }

    // @Test
    public void test() {

        byte[] bytes;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            long[] l = new long[]{1, 2};
            Hessian2Output output = new Hessian2Output(bos);
            output.writeObject(l);
            output.flush();
            bytes = bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            Hessian2Input input = new Hessian2Input(bis);
            long[] obj = (long[]) input.readObject();
            input.close();

            for (long l : obj) {
                System.out.println(l);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
