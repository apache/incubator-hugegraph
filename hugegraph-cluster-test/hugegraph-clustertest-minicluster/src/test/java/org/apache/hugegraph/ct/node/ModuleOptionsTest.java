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

package org.apache.hugegraph.ct.node;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ModuleOptionsTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testPDUsesPackagedModuleOptions() throws Exception {
        CapturingPD wrapper = new CapturingPD();
        Path options = prepareDistribution(wrapper);
        wrapper.start();
        Assert.assertTrue(wrapper.command.contains("@" + options.toAbsolutePath()));
        Assert.assertFalse(wrapper.command.stream().anyMatch(arg -> arg.startsWith("--add-opens")));
        Assert.assertTrue(wrapper.instance.waitFor(10, TimeUnit.SECONDS));
        Files.delete(options);
        AssertionError error = Assert.assertThrows(AssertionError.class, wrapper::start);
        Assert.assertTrue(error.getMessage().contains("JVM module options"));
    }

    @Test
    public void testStoreUsesPackagedModuleOptions() throws Exception {
        CapturingStore wrapper = new CapturingStore();
        Path options = prepareDistribution(wrapper);
        wrapper.start();
        Assert.assertTrue(wrapper.command.contains("@" + options.toAbsolutePath()));
        Assert.assertFalse(wrapper.command.stream().anyMatch(arg -> arg.startsWith("--add-opens")));
        Assert.assertTrue(wrapper.instance.waitFor(10, TimeUnit.SECONDS));
        Files.delete(options);
        AssertionError error = Assert.assertThrows(AssertionError.class, wrapper::start);
        Assert.assertTrue(error.getMessage().contains("JVM module options"));
    }

    private Path prepareDistribution(AbstractNodeWrapper wrapper) throws Exception {
        Path distribution = temporaryFolder.newFolder().toPath();
        Files.createDirectory(distribution.resolve("lib"));
        Files.createDirectory(distribution.resolve("bin"));
        Path options = distribution.resolve("bin/jvm-module.options");
        Files.writeString(options, "--add-opens=java.base/java.lang=ALL-UNNAMED\n");
        wrapper.updateWorkPath(distribution.resolve("lib").toString() + File.separator);
        return options;
    }

    private static ProcessBuilder harmlessProcess() {
        return new ProcessBuilder(Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
                                  "-version");
    }

    private static class CapturingPD extends PDNodeWrapper {

        private List<String> command;

        @Override
        public void createNodeDir(Path source, String destination) {
            // Distribution files are provided by the test.
        }

        @Override
        public void createLogDir() {
            // No service process is launched.
        }

        @Override
        protected ProcessBuilder runCmd(List<String> command, File stdout) throws IOException {
            this.command = command;
            return harmlessProcess();
        }
    }

    private static class CapturingStore extends StoreNodeWrapper {

        private List<String> command;

        @Override
        public void createNodeDir(Path source, String destination) {
            // Distribution files are provided by the test.
        }

        @Override
        public void createLogDir() {
            // No service process is launched.
        }

        @Override
        protected ProcessBuilder runCmd(List<String> command, File stdout) throws IOException {
            this.command = command;
            return harmlessProcess();
        }
    }
}
