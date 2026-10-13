/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.unit.auth;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.stream.Stream;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.HugeUser;
import org.apache.hugegraph.auth.StandardAuthManager;
import org.apache.hugegraph.auth.StandardAuthenticator;
import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.dist.RegisterUtil;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.testutil.Whitebox;
import org.apache.hugegraph.util.StringEncoding;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * auth.admin_pa goes through the properties grammar, so a padded, escaped or
 * non-ASCII password can reach the server changed. auth.admin_pa_file is read
 * as raw UTF-8 and must hand back exactly what the operator wrote.
 */
public class AdminPasswordFileTest {

    private Path workDir;

    @BeforeClass
    public static void registerOptions() {
        RegisterUtil.registerServer();
    }

    @Before
    public void setup() throws IOException {
        this.workDir = Files.createTempDirectory("admin-password-file-test");
    }

    @After
    public void teardown() throws IOException {
        try (Stream<Path> paths = Files.walk(this.workDir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder())
                                  .toArray(Path[]::new)) {
                Files.deleteIfExists(path);
            }
        }
    }

    @Test
    public void testFallsBackToAdminPa() {
        Assert.assertEquals("pa", initialAdminPassword(null, null));
        Assert.assertEquals("secret", initialAdminPassword(null, "secret"));
    }

    @Test
    public void testFileKeepsCharactersThePropertiesReaderChanges()
                                                        throws IOException {
        for (String password : Arrays.asList("  padded  ", "two\\back",
                                             "x\ty", "pässword",
                                             "密码 ☃", "a:b=c#d")) {
            Path file = this.write(password.getBytes(StandardCharsets.UTF_8));
            Assert.assertEquals(password, initialAdminPassword(file, null));
        }
    }

    @Test
    public void testFileTakesPrecedenceOverAdminPa() throws IOException {
        Path file = this.write("from-file".getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals("from-file", initialAdminPassword(file, "from-pa"));
    }

    @Test
    public void testFileDropsOneTrailingLineBreak() throws IOException {
        Assert.assertEquals("secret", initialAdminPassword(
                this.write(utf8("secret\n")), null));
        Assert.assertEquals("secret", initialAdminPassword(
                this.write(utf8("secret\r\n")), null));
        Assert.assertEquals("secret\n", initialAdminPassword(
                this.write(utf8("secret\n\n")), null));
        Assert.assertEquals("secret\r", initialAdminPassword(
                this.write(utf8("secret\r")), null));
    }

    @Test
    public void testEmptyFileFails() throws IOException {
        for (String content : Arrays.asList("", "\n", "\r\n")) {
            Path file = this.write(utf8(content));
            Assert.assertThrows(ConfigException.class, () -> {
                initialAdminPassword(file, "secret");
            }, e -> Assert.assertContains("is empty", e.getMessage()));
        }
    }

    @Test
    public void testMissingFileFails() {
        Path file = this.workDir.resolve("missing");
        Assert.assertThrows(ConfigException.class, () -> {
            initialAdminPassword(file, "secret");
        }, e -> Assert.assertContains("Failed to read", e.getMessage()));
    }

    /**
     * Guessing another charset is what turned 'pässword' into 'pÃ¤ssword'
     * in the first place, so bytes that are not UTF-8 are refused.
     */
    @Test
    public void testNonUtf8FileFails() throws IOException {
        Path file = this.write("pässword".getBytes(StandardCharsets.ISO_8859_1));
        Assert.assertThrows(ConfigException.class, () -> {
            initialAdminPassword(file, null);
        }, e -> Assert.assertContains("not valid UTF-8", e.getMessage()));
    }

    /**
     * PowerShell and some Windows editors write a BOM before UTF-8 text. Kept,
     * it would become an invisible first character of the admin password.
     */
    @Test
    public void testFileDropsLeadingBom() throws IOException {
        Assert.assertEquals("secret", initialAdminPassword(
                this.write(utf8("\ufeffsecret\r\n")), null));
        Assert.assertThrows(ConfigException.class, () -> {
            initialAdminPassword(this.write(utf8("\ufeff\n")), "secret");
        }, e -> Assert.assertContains("is empty", e.getMessage()));
    }

    /**
     * A path pointing at a large file must not be read whole into memory.
     */
    @Test
    public void testOversizedFileFails() throws IOException {
        byte[] content = new byte[4097];
        Arrays.fill(content, (byte) 'a');
        Path file = this.write(content);
        Assert.assertThrows(ConfigException.class, () -> {
            initialAdminPassword(file, "secret");
        }, e -> Assert.assertContains("larger than 4096 bytes", e.getMessage()));
    }

    /**
     * jBCrypt hashes only the first 72 bytes, so a longer password would also
     * accept every password sharing that prefix. The limit counts UTF-8 bytes
     * after the BOM and line break are dropped.
     */
    @Test
    public void testPasswordOverBcryptLimitFails() throws IOException {
        String ascii = "a".repeat(72);
        String umlauts = "\u00e4".repeat(36);
        Assert.assertEquals(ascii, initialAdminPassword(
                this.write(utf8("\uFEFF" + ascii + "\r\n")), null));
        Assert.assertEquals(umlauts, initialAdminPassword(
                this.write(utf8(umlauts)), null));

        for (String password : Arrays.asList(ascii + "a", umlauts + "a")) {
            Path file = this.write(utf8(password));
            Assert.assertThrows(ConfigException.class, () -> {
                initialAdminPassword(file, "secret");
            }, e -> Assert.assertContains("longer than 72 bytes", e.getMessage()));
        }
    }

    @Test
    public void testDirectoryFails() {
        Assert.assertThrows(ConfigException.class, () -> {
            initialAdminPassword(this.workDir, "secret");
        }, e -> Assert.assertContains("not a regular file", e.getMessage()));
    }

    /**
     * Opening a FIFO blocks until a writer connects, so it must be refused
     * from its attributes before it is opened. The timeout turns a
     * regression into a failure instead of a hung build.
     */
    @Test(timeout = 10000)
    public void testFifoFailsWithoutBlocking() throws Exception {
        Path fifo = this.workDir.resolve("fifo");
        int exit;
        try {
            exit = new ProcessBuilder("mkfifo", fifo.toString()).start()
                                                               .waitFor();
        } catch (IOException e) {
            exit = -1;
        }
        Assume.assumeTrue("mkfifo is not available", exit == 0);

        Assert.assertThrows(ConfigException.class, () -> {
            initialAdminPassword(fifo, "secret");
        }, e -> Assert.assertContains("not a regular file", e.getMessage()));
    }

    /**
     * Kubernetes mounts each Secret key as a symlink into a ..data directory,
     * so the regular-file check has to follow links.
     */
    @Test
    public void testSymlinkToRegularFileIsFollowed() throws IOException {
        Path target = this.write(utf8("secret\n"));
        Path link = this.workDir.resolve("link");
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException e) {
            Assume.assumeNoException("symlinks are not supported", e);
        }
        Assert.assertEquals("secret", initialAdminPassword(link, null));
    }

    /**
     * init-store prompts on stdin for a persistent backend. With a password
     * file configured it must create the admin from the file instead.
     */
    @Test
    public void testInitStoreTakesFileInsteadOfPrompt() throws Exception {
        Path file = this.write(utf8("p\u00e4ssword\n"));
        StandardAuthManager authManager = Mockito.mock(StandardAuthManager.class);
        HugeGraph graph = mockAuthGraph(authManager);

        initStoreAdminUser(graph, config(file, null));

        ArgumentCaptor<HugeUser> admin = ArgumentCaptor.forClass(HugeUser.class);
        Mockito.verify(authManager).createUser(admin.capture());
        Assert.assertTrue(StringEncoding.checkPassword("p\u00e4ssword",
                                                       admin.getValue().password()));
        Mockito.verify(graph).close();
    }

    /**
     * init-store does not close an hstore auth graph itself, so a refused
     * file must still close it rather than leave the backend open on exit.
     */
    @Test
    public void testInitStoreClosesGraphWhenFileIsRefused() throws Exception {
        StandardAuthManager authManager = Mockito.mock(StandardAuthManager.class);
        HugeGraph graph = mockAuthGraph(authManager);
        HugeConfig config = config(this.workDir.resolve("missing"), "secret");

        Assert.assertThrows(ConfigException.class, () -> {
            initStoreAdminUser(graph, config);
        }, e -> Assert.assertContains("Failed to read", e.getMessage()));
        Mockito.verify(authManager, Mockito.never()).createUser(Mockito.any());
        Mockito.verify(graph).close();
    }

    /**
     * The refused file is what the operator has to fix, so a failure while
     * closing the graph afterwards must not replace it.
     */
    @Test
    public void testInitStoreCloseFailureKeepsFileError() throws Exception {
        StandardAuthManager authManager = Mockito.mock(StandardAuthManager.class);
        HugeGraph graph = mockAuthGraph(authManager);
        IllegalStateException closeError = new IllegalStateException("close");
        Mockito.doThrow(closeError).when(graph).close();
        HugeConfig config = config(this.workDir.resolve("missing"), "secret");

        Assert.assertThrows(ConfigException.class, () -> {
            initStoreAdminUser(graph, config);
        }, e -> {
            Assert.assertContains("Failed to read", e.getMessage());
            Assert.assertEquals(closeError, e.getSuppressed()[0]);
        });
    }

    private static HugeGraph mockAuthGraph(StandardAuthManager authManager) {
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.hugegraph()).thenReturn(graph);
        Mockito.when(graph.authManager()).thenReturn(authManager);
        return graph;
    }

    /**
     * stdin is empty, so a regression that falls back to the prompt fails
     * with NoSuchElementException instead of blocking the test run.
     */
    private static void initStoreAdminUser(HugeGraph graph, HugeConfig config) {
        StandardAuthenticator authenticator = new StandardAuthenticator();
        Whitebox.setInternalState(authenticator, "graph", graph);
        InputStream stdin = System.in;
        System.setIn(new ByteArrayInputStream(new byte[0]));
        try {
            Whitebox.invoke(StandardAuthenticator.class,
                            new Class<?>[]{HugeConfig.class}, "initAdminUser",
                            authenticator, config);
        } finally {
            System.setIn(stdin);
        }
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private Path write(byte[] content) throws IOException {
        Path file = Files.createTempFile(this.workDir, "admin", ".pa");
        Files.write(file, content);
        return file;
    }

    private static String initialAdminPassword(Path file, String adminPa) {
        return StandardAuthenticator.initialAdminPassword(config(file, adminPa));
    }

    private static HugeConfig config(Path file, String adminPa) {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        if (file != null) {
            properties.setProperty(ServerOptions.ADMIN_PA_FILE.name(),
                                   file.toString());
        }
        if (adminPa != null) {
            properties.setProperty(ServerOptions.ADMIN_PA.name(), adminPa);
        }
        return new HugeConfig(properties);
    }
}
