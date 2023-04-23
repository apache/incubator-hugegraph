package client;

import org.junit.Assert;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

/**
 * @author zhangyingjie
 * @date 2022/10/18
 **/
@Slf4j
public class ClientTest {

     @Test
    public void testDemo() {
        String s = "i am client";
        log.info("UT:{}", s);
        Assert.assertTrue(s.startsWith("i"));
    }
}
