package com.baidu.hugegraph.store.boot;

import com.alipay.remoting.util.StringUtils;
import com.baidu.hugegraph.store.node.StoreNodeApplication;
import org.springframework.boot.SpringApplication;

import java.io.File;

public class StoreNodeServer03 {

    public static void main(String[] args) {
      // deleteDir(new File("tmp/8503"));
        String logPath = System.getProperty("logging.path");
        if (StringUtils.isBlank(logPath)) {
            System.setProperty("logging.path", "logs/8503");
            System.setProperty("com.alipay.remoting.client.log.level", "WARN");
        }
        if (System.getProperty("bolt.channel_write_buf_low_water_mark") == null)
            System.setProperty("bolt.channel_write_buf_low_water_mark", Integer.toString(4 * 1024 * 1024));
        if (System.getProperty("bolt.channel_write_buf_high_water_mark") == null)
            System.setProperty("bolt.channel_write_buf_high_water_mark", Integer.toString(8 * 1024 * 1024));

        SpringApplication.run(StoreNodeApplication.class, "--spring.profiles.active=server03");
        System.out.println("StoreNodeServer03 started.");
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }
}