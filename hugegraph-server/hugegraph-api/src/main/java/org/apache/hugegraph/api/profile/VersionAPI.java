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

package org.apache.hugegraph.api.profile;

import java.util.Map;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.security.PermitAll;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;

import org.apache.hugegraph.api.API;
import org.apache.hugegraph.version.ApiVersion;
import org.apache.hugegraph.version.CoreVersion;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;

@Path("versions")
@Singleton
@Tag(name = "VersionAPI")
public class VersionAPI extends API {

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    @PermitAll
    public Object list() {
        Map<String, String> versions = ImmutableMap.of("version", "v1",
                                       "core", CoreVersion.VERSION.toString(),
                                       "gremlin", CoreVersion.GREMLIN_VERSION,
                                       "api", ApiVersion.VERSION.toString());
        return ImmutableMap.of("versions", versions);
    }
}
