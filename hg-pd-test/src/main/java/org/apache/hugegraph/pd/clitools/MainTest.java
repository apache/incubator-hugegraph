package org.apache.hugegraph.pd.clitools;

import com.baidu.hugegraph.pd.clitools.Main;
import com.baidu.hugegraph.pd.common.PDException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

@Slf4j
public class MainTest extends BaseCliToolsTest{


    @Test
    public void getConfig() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "enableBatchLoad"});
    }
    // @Test
    public void setBatchTrue() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "enableBatchLoad= true "});
    }

    //    @Test
    public void setBatchFalse() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "enableBatchLoad=false"});
    }

    @Test
    public void getConfig2() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "shardCount"});
    }
    //    @Test
    public void setShardCount1() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "shardCount=1"});
    }

    //    @Test
    public void setShardCount3() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "shardCount=3"});
    }

    @Test
    public void test2(){
        Integer[] a = new Integer[] { 1, 0, 3, 2};
        List<Integer> aa = Arrays.asList(a);
        System.out.printf(test2sup(aa, aa.size(), 0) ? "TRUE" : "FALSE");
    }
    public static boolean test2sup (List<Integer> arrays, int tail, int res) {
        System.out.println(String.format("%d %d", tail, res));
        if (tail == 0) {
            System.out.println(String.format("a = %d %d", tail, res));
            return false;
        } else if (tail == 1) {
            System.out.println(String.format("b = %d %d", arrays.get(0), res));
            return (arrays.get(0) == res);
        } else if (tail == 2) {
            System.out.println(String.format("c = %d %d %d", arrays.get(0), arrays.get(1), res));
            return (arrays.get(0) + arrays.get(1) == Math.abs(res)) ||
                    (Math.abs(arrays.get(0) - arrays.get(1)) == Math.abs(res));
        } else {
            return test2sup(arrays, tail - 1, res + arrays.get(tail - 1)) ||
                    test2sup(arrays, tail - 1, res - arrays.get(tail - 1));
        }
    }


}