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

import java.io.IOException;
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
     * init-store prompts on stdin for a persistent backend. With a password
     * file configured it must create the admin from the file instead.
     */
    @Test
    public void testInitStoreTakesFileInsteadOfPrompt() throws Exception {
        Path file = this.write(utf8("p\u00e4ssword\n"));
        StandardAuthManager authManager = Mockito.mock(StandardAuthManager.class);
        HugeGraph graph = Mockito.mock(HugeGraph.class);
        Mockito.when(graph.hugegraph()).thenReturn(graph);
        Mockito.when(graph.authManager()).thenReturn(authManager);
        StandardAuthenticator authenticator = new StandardAuthenticator();
        Whitebox.setInternalState(authenticator, "graph", graph);

        Whitebox.invoke(StandardAuthenticator.class,
                        new Class<?>[]{HugeConfig.class}, "initAdminUser",
                        authenticator, config(file, null));

        ArgumentCaptor<HugeUser> admin = ArgumentCaptor.forClass(HugeUser.class);
        Mockito.verify(authManager).createUser(admin.capture());
        Assert.assertTrue(StringEncoding.checkPassword("p\u00e4ssword",
                                                       admin.getValue().password()));
        Mockito.verify(graph).close();
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
