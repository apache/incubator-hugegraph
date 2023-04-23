package core;

import org.junit.Assert;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2022/10/18
 **/
@Slf4j
public class CoreTest {

     @Test
    public void testDemo() {
        String s = "i am core";
        log.info("UT:{}", s);
        Assert.assertTrue(s.startsWith("i"));
    }
}
