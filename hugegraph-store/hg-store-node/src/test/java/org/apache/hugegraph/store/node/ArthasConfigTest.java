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

package org.apache.hugegraph.store.node;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

public class ArthasConfigTest {

    @Test
    public void testDefaults() throws Exception {
        assertOptions(Collections.emptyMap(), "8566", "8565", "jad");
        Assertions.assertEquals("0.0.0.0", resolve(Collections.emptyMap(), "arthasip"));
    }

    @Test
    public void testCanonicalKeys() throws Exception {
        assertOptions(properties("telnet_port", "http_port", "disabled_commands"),
                      "18566", "18565", "jad,stop");
    }

    @Test
    public void testLegacyKeys() throws Exception {
        assertOptions(properties("telnetPort", "httpPort", "disabledCommands"),
                      "18566", "18565", "jad,stop");
    }

    @Test
    public void testCanonicalKeysTakePrecedence() throws Exception {
        Map<String, Object> properties = properties("telnet_port", "http_port", "disabled_commands");
        properties.put("arthas.telnetPort", "28566");
        properties.put("arthas.httpPort", "28565");
        properties.put("arthas.disabledCommands", "stop");
        assertOptions(properties, "18566", "18565", "jad,stop");
    }

    @Test
    public void testExplicitCanonicalDefaultsTakePrecedence() throws Exception {
        Map<String, Object> properties = properties("telnetPort", "httpPort", "disabledCommands");
        properties.put("arthas.telnet_port", "8566");
        properties.put("arthas.http_port", "8565");
        properties.put("arthas.disabled_commands", "jad");
        assertOptions(properties, "8566", "8565", "jad");
    }

    @Test
    public void testIpOverrideIsPreserved() throws Exception {
        Assertions.assertEquals("127.0.0.1",
                                resolve(Collections.singletonMap("arthas.ip", "127.0.0.1"), "arthasip"));
    }

    private static Map<String, Object> properties(String telnet, String http, String commands) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("arthas." + telnet, "18566");
        properties.put("arthas." + http, "18565");
        properties.put("arthas." + commands, "jad,stop");
        return properties;
    }

    private static void assertOptions(Map<String, Object> properties, String telnet, String http,
                                      String commands) throws Exception {
        Assertions.assertEquals(telnet, resolve(properties, "telnetPort"));
        Assertions.assertEquals(http, resolve(properties, "httpPort"));
        Assertions.assertEquals(commands, resolve(properties, "disCmd"));
    }

    private static String resolve(Map<String, Object> properties, String field) throws Exception {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().addFirst(new MapPropertySource("test", properties));
        Value annotation = AppConfig.ArthasConfig.class.getDeclaredField(field).getAnnotation(Value.class);
        return environment.resolveRequiredPlaceholders(annotation.value());
    }
}
