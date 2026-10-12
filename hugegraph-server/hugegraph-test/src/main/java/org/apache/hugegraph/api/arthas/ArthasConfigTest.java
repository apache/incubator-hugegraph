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

package org.apache.hugegraph.api.arthas;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.OptionSpace;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.testutil.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ArthasConfigTest {

    @BeforeClass
    public static void registerOptions() {
        OptionSpace.register("server", ServerOptions.instance());
    }

    @Test
    public void testDefaults() {
        Map<String, String> config = ArthasAPI.arthasConfig(new HugeConfig(Collections.emptyMap()));
        assertOptions(config, "8562", "8561", "jad");
        Assert.assertEquals("0.0.0.0", config.get("arthas.ip"));
    }

    @Test
    public void testDistributionKeys() {
        Map<String, Object> properties = properties("telnet_port", "http_port", "disabled_commands");
        properties.put("arthas.ip", "127.0.0.1");
        Map<String, String> config = ArthasAPI.arthasConfig(new HugeConfig(properties));
        assertOptions(config, "18562", "18561", "jad,stop");
        Assert.assertEquals("127.0.0.1", config.get("arthas.ip"));
    }

    @Test
    public void testLegacyKeys() {
        Map<String, Object> properties = properties("telnetPort", "httpPort", "disabledCommands");
        assertOptions(ArthasAPI.arthasConfig(new HugeConfig(properties)), "18562", "18561", "jad,stop");
    }

    @Test
    public void testCanonicalKeysTakePrecedence() {
        Map<String, Object> properties = properties("telnet_port", "http_port", "disabled_commands");
        properties.put("arthas.telnetPort", "28562");
        properties.put("arthas.httpPort", "28561");
        properties.put("arthas.disabledCommands", "stop");
        assertOptions(ArthasAPI.arthasConfig(new HugeConfig(properties)), "18562", "18561", "jad,stop");
    }

    @Test
    public void testExplicitCanonicalDefaultsTakePrecedence() {
        Map<String, Object> properties = properties("telnetPort", "httpPort", "disabledCommands");
        properties.put("arthas.telnet_port", "8562");
        properties.put("arthas.http_port", "8561");
        properties.put("arthas.disabled_commands", "jad");
        assertOptions(ArthasAPI.arthasConfig(new HugeConfig(properties)), "8562", "8561", "jad");
    }

    @Test
    public void testCanonicalAndLegacyKeysAreValidated() {
        for (String key : new String[]{"telnet_port", "telnetPort", "http_port", "httpPort",
                                       "disabled_commands", "disabledCommands"}) {
            Assert.assertThrows(ConfigException.class,
                                () -> new HugeConfig(Collections.singletonMap("arthas." + key, "")));
        }
    }

    private static Map<String, Object> properties(String telnet, String http, String commands) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("arthas." + telnet, "18562");
        properties.put("arthas." + http, "18561");
        properties.put("arthas." + commands, "jad,stop");
        return properties;
    }

    private static void assertOptions(Map<String, String> config, String telnet, String http,
                                      String commands) {
        Assert.assertEquals(4, config.size());
        Assert.assertEquals(telnet, config.get("arthas.telnetPort"));
        Assert.assertEquals(http, config.get("arthas.httpPort"));
        Assert.assertEquals(commands, config.get("arthas.disabledCommands"));
    }
}
