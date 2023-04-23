package com.baidu.hugegraph.store.boot;

import com.alipay.remoting.util.StringUtils;
import com.baidu.hugegraph.store.node.StoreNodeApplication;
import org.springframework.boot.SpringApplication;
import java.io.File;

public class StoreNodeServer00 {

    public static void main(String[] args) {
        // deleteDir(new File("tmp/8500"));
        String logPath = System.getProperty("logging.path");
        if (StringUtils.isBlank(logPath)) {
            System.setProperty("logging.path", "logs/8500");
            System.setProperty("com.alipay.remoting.client.log.level", "WARN");
        }
        if (System.getProperty("bolt.channel_write_buf_low_water_mark") == null)
            System.setProperty("bolt.channel_write_buf_low_water_mark", Integer.toString(4 * 1024 * 1024));
        if (System.getProperty("bolt.channel_write_buf_high_water_mark") == null)
            System.setProperty("bolt.channel_write_buf_high_water_mark", Integer.toString(8 * 1024 * 1024));
        SpringApplication.run(StoreNodeApplication.class,"--spring.profiles.active=server00");
        System.out.println("StoreNodeServer00 started.");
    }
    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for(File file : dir.listFiles()){
                deleteDir(file);
            }
        }
        return dir.delete();
    }
}