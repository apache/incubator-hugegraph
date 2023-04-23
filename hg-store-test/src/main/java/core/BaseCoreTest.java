package core;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;


public class BaseCoreTest {
    @BeforeClass
    public static void init() throws Exception {

    }

    @After
    public void teardown() throws Exception {
        // pass
    }

    public static void deleteDirectory(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            System.out.println(String.format("Failed to start ....,%s", e.getMessage()));
        }
    }
}