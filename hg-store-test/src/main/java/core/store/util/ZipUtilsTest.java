package core.store.util;

import com.baidu.hugegraph.store.util.ZipUtils;
import org.apache.logging.log4j.core.util.FileUtils;
import org.junit.Before;
import org.junit.Test;
import util.UnitTestBase;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.CRC32;

import static org.junit.Assert.assertTrue;

public class ZipUtilsTest {
    private static final String ZIP_TEST_PATH = "/tmp/zip_util_test";

    @Before
    public void init() throws IOException {
        UnitTestBase.deleteDir(new File(ZIP_TEST_PATH));
        FileUtils.mkdir(new File(ZIP_TEST_PATH), true);
        FileUtils.mkdir(new File(ZIP_TEST_PATH + "/input"), true);
        FileUtils.mkdir(new File(ZIP_TEST_PATH + "/output"), true);
        Files.createFile(Paths.get(ZIP_TEST_PATH + "/input/foo.txt"));
    }

    @Test
    public void testZip() throws IOException {
        ZipUtils.compress(ZIP_TEST_PATH, "input", ZIP_TEST_PATH + "/foo.zip", new CRC32());
        ZipUtils.decompress(ZIP_TEST_PATH + "/foo.zip", ZIP_TEST_PATH + "/output", new CRC32());
        assertTrue(Files.exists(Paths.get(ZIP_TEST_PATH + "/output/input/foo.txt")));
    }
}
