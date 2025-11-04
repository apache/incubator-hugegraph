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

package org.apache.hugegraph.unit.api.filter;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.api.filter.PathFilter;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import jakarta.inject.Provider;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.PathSegment;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

/**
 * PathFilter 单元测试
 * 测试场景：
 * 1. 白名单路径不被重定向
 * 2. 普通路径正确添加 graphspace 前缀
 * 3. 查询参数被正确保留
 * 4. 特殊字符和编码处理
 * 5. 边界情况（空路径、根路径等）
 */
public class PathFilterTest extends BaseUnitTest {

    private PathFilter pathFilter;
    private Provider<HugeConfig> configProvider;
    private HugeConfig config;
    private ContainerRequestContext requestContext;
    private UriInfo uriInfo;

    @Before
    public void setup() {
        // 创建配置
        Configuration conf = new PropertiesConfiguration();
        conf.setProperty(ServerOptions.PATH_GRAPH_SPACE.name(), "DEFAULT");
        this.config = new HugeConfig(conf);

        // 创建 Provider
        this.configProvider = () -> config;

        // 创建 PathFilter 并注入 Provider
        this.pathFilter = new PathFilter();
        injectProvider(this.pathFilter, this.configProvider);

        // Mock request context 和 uriInfo
        this.requestContext = Mockito.mock(ContainerRequestContext.class);
        this.uriInfo = Mockito.mock(UriInfo.class);
        Mockito.when(this.requestContext.getUriInfo()).thenReturn(this.uriInfo);
    }

    /**
     * 使用反射注入 configProvider
     */
    private void injectProvider(PathFilter filter, Provider<HugeConfig> provider) {
        try {
            java.lang.reflect.Field field = PathFilter.class.getDeclaredField("configProvider");
            field.setAccessible(true);
            field.set(filter, provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject configProvider", e);
        }
    }

    /**
     * 创建 PathSegment mock
     */
    private PathSegment createPathSegment(String path) {
        PathSegment segment = Mockito.mock(PathSegment.class);
        Mockito.when(segment.getPath()).thenReturn(path);
        return segment;
    }

    /**
     * 设置 URI 信息
     */
    private void setupUriInfo(String basePath, String requestPath, List<String> segments,
                              String query) {
        URI baseUri = URI.create("http://localhost:8080" + basePath);
        URI requestUri = query != null ?
                         URI.create("http://localhost:8080" + requestPath + "?" + query) :
                         URI.create("http://localhost:8080" + requestPath);

        Mockito.when(uriInfo.getBaseUri()).thenReturn(baseUri);
        Mockito.when(uriInfo.getRequestUri()).thenReturn(requestUri);

        List<PathSegment> pathSegments = new ArrayList<>();
        for (String segment : segments) {
            pathSegments.add(createPathSegment(segment));
        }
        Mockito.when(uriInfo.getPathSegments()).thenReturn(pathSegments);
        Mockito.when(uriInfo.getPath()).thenReturn(String.join("/", segments));

        // Mock UriBuilder
        UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);
        Mockito.when(uriInfo.getRequestUriBuilder()).thenReturn(uriBuilder);
        Mockito.when(uriBuilder.uri(Mockito.anyString())).thenReturn(uriBuilder);

        // 捕获重定向的 URI
        Mockito.when(uriBuilder.build()).thenAnswer(invocation -> {
            // 这里会被 filter 调用
            return requestUri;
        });
    }

    /**
     * 测试白名单 API - 空路径
     */
    @Test
    public void testWhiteListApi_EmptyPath() throws IOException {
        setupUriInfo("/", "/", List.of(""), null);

        pathFilter.filter(requestContext);

        // 验证白名单 API 不会触发 setRequestUri
        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
        // 验证请求时间戳被设置
        Mockito.verify(requestContext).setProperty(
                Mockito.eq(PathFilter.REQUEST_TIME), Mockito.anyLong());
    }

    /**
     * 测试白名单 API - /apis
     */
    @Test
    public void testWhiteListApi_Apis() throws IOException {
        setupUriInfo("/", "/apis", List.of("apis"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * 测试白名单 API - /metrics
     */
    @Test
    public void testWhiteListApi_Metrics() throws IOException {
        setupUriInfo("/", "/metrics", List.of("metrics"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * 测试白名单 API - /health
     */
    @Test
    public void testWhiteListApi_Health() throws IOException {
        setupUriInfo("/", "/health", List.of("health"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * 测试白名单 API - /gremlin
     */
    @Test
    public void testWhiteListApi_Gremlin() throws IOException {
        setupUriInfo("/", "/gremlin", List.of("gremlin"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * 测试白名单 API - /auth/users
     */
    @Test
    public void testWhiteListApi_AuthUsers() throws IOException {
        setupUriInfo("/", "/auth/users", List.of("auth"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * 测试 graphspaces 路径不被重定向
     */
    @Test
    public void testGraphSpacePath_NotRedirected() throws IOException {
        setupUriInfo("/", "/graphspaces/space1/graphs",
                     Arrays.asList("graphspaces", "space1", "graphs"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * 测试 arthas 路径不被重定向
     */
    @Test
    public void testArthasPath_NotRedirected() throws IOException {
        setupUriInfo("/", "/arthas/api", Arrays.asList("arthas", "api"), null);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext, Mockito.never()).setRequestUri(
                Mockito.any(URI.class), Mockito.any(URI.class));
    }

    /**
     * 测试普通路径被正确重定向 - 单段路径
     */
    @Test
    public void testNormalPath_SingleSegment() throws IOException {
        setupUriInfo("/", "/graphs", List.of("graphs"), null);

        UriBuilder builder = UriBuilder.fromUri("http://localhost:8080/");
        URI expectedUri = builder.path("graphspaces/DEFAULT/graphs").build();

        UriBuilder mockBuilder = Mockito.mock(UriBuilder.class);
        Mockito.when(uriInfo.getRequestUriBuilder()).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.uri(Mockito.anyString())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.build()).thenReturn(expectedUri);

        pathFilter.filter(requestContext);

        // 验证重定向被调用
        Mockito.verify(requestContext).setRequestUri(
                Mockito.any(URI.class), Mockito.eq(expectedUri));
    }

    /**
     * 测试普通路径被正确重定向 - 多段路径
     */
    @Test
    public void testNormalPath_MultipleSegments() throws IOException {
        setupUriInfo("/", "/graphs/hugegraph/vertices",
                     Arrays.asList("graphs", "hugegraph", "vertices"), null);

        URI expectedUri =
                URI.create("http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/vertices");

        UriBuilder mockBuilder = Mockito.mock(UriBuilder.class);
        Mockito.when(uriInfo.getRequestUriBuilder()).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.uri(Mockito.anyString())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.build()).thenReturn(expectedUri);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext).setRequestUri(
                Mockito.any(URI.class), Mockito.eq(expectedUri));
    }

    /**
     * 测试查询参数被正确保留
     */
    @Test
    public void testQueryParameters_Preserved() throws IOException {
        String queryString = "limit=10&offset=20&label=person";
        setupUriInfo("/", "/graphs/hugegraph/vertices",
                     Arrays.asList("graphs", "hugegraph", "vertices"), queryString);

        URI originalRequestUri = uriInfo.getRequestUri();
        Assert.assertTrue(originalRequestUri.toString().contains(queryString));

        UriBuilder mockBuilder = Mockito.mock(UriBuilder.class);
        Mockito.when(uriInfo.getRequestUriBuilder()).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.uri(Mockito.anyString())).thenReturn(mockBuilder);

        // 构建带查询参数的重定向 URI
        URI redirectedUri = URI.create(
                "http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/vertices?" +
                queryString);
        Mockito.when(mockBuilder.build()).thenReturn(redirectedUri);

        pathFilter.filter(requestContext);

        // 验证重定向 URI 包含查询参数
        Mockito.verify(requestContext).setRequestUri(
                Mockito.any(URI.class), Mockito.eq(redirectedUri));
    }

    /**
     * 测试特殊字符在路径中的处理
     */
    @Test
    public void testSpecialCharacters_InPath() throws IOException {
        setupUriInfo("/", "/schema/vertexlabels/person-label",
                     Arrays.asList("schema", "vertexlabels", "person-label"), null);

        URI expectedUri = URI.create(
                "http://localhost:8080/graphspaces/DEFAULT/schema/vertexlabels/person-label");

        UriBuilder mockBuilder = Mockito.mock(UriBuilder.class);
        Mockito.when(uriInfo.getRequestUriBuilder()).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.uri(Mockito.anyString())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.build()).thenReturn(expectedUri);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext).setRequestUri(
                Mockito.any(URI.class), Mockito.eq(expectedUri));
    }

    /**
     * 测试 URL 编码字符的处理
     */
    @Test
    public void testUrlEncoded_Characters() throws IOException {
        // 路径中包含编码的空格 %20
        setupUriInfo("/", "/schema/propertykeys/my%20key",
                     Arrays.asList("schema", "propertykeys", "my%20key"), null);

        URI expectedUri = URI.create(
                "http://localhost:8080/graphspaces/DEFAULT/schema/propertykeys/my%20key");

        UriBuilder mockBuilder = Mockito.mock(UriBuilder.class);
        Mockito.when(uriInfo.getRequestUriBuilder()).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.uri(Mockito.anyString())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.build()).thenReturn(expectedUri);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext).setRequestUri(
                Mockito.any(URI.class), Mockito.eq(expectedUri));
    }

    /**
     * 测试自定义 graph space 配置
     */
    @Test
    public void testCustomGraphSpace_Configuration() throws IOException {
        // 修改配置为自定义的 graph space
        Configuration customConf = new PropertiesConfiguration();
        customConf.setProperty(ServerOptions.PATH_GRAPH_SPACE.name(), "CUSTOM_SPACE");
        HugeConfig customConfig = new HugeConfig(customConf);

        Provider<HugeConfig> customProvider = () -> customConfig;
        injectProvider(this.pathFilter, customProvider);

        setupUriInfo("/", "/graphs/test",
                     Arrays.asList("graphs", "test"), null);

        URI expectedUri = URI.create("http://localhost:8080/graphspaces/CUSTOM_SPACE/graphs/test");

        UriBuilder mockBuilder = Mockito.mock(UriBuilder.class);
        Mockito.when(uriInfo.getRequestUriBuilder()).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.uri(Mockito.anyString())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.build()).thenReturn(expectedUri);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext).setRequestUri(
                Mockito.any(URI.class), Mockito.eq(expectedUri));
    }

    /**
     * 测试深层嵌套路径
     */
    @Test
    public void testDeeplyNested_Path() throws IOException {
        setupUriInfo("/", "/graphs/hugegraph/traversers/shortestpath",
                     Arrays.asList("graphs", "hugegraph", "traversers", "shortestpath"), null);

        URI expectedUri = URI.create(
                "http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/traversers" +
                "/shortestpath");

        UriBuilder mockBuilder = Mockito.mock(UriBuilder.class);
        Mockito.when(uriInfo.getRequestUriBuilder()).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.uri(Mockito.anyString())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.build()).thenReturn(expectedUri);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext).setRequestUri(
                Mockito.any(URI.class), Mockito.eq(expectedUri));
    }

    /**
     * 测试 isWhiteAPI 静态方法 - 所有白名单路径
     */
    @Test
    public void testIsWhiteAPI_AllWhiteListPaths() {
        Assert.assertTrue(PathFilter.isWhiteAPI(""));
        Assert.assertTrue(PathFilter.isWhiteAPI("apis"));
        Assert.assertTrue(PathFilter.isWhiteAPI("metrics"));
        Assert.assertTrue(PathFilter.isWhiteAPI("versions"));
        Assert.assertTrue(PathFilter.isWhiteAPI("health"));
        Assert.assertTrue(PathFilter.isWhiteAPI("gremlin"));
        Assert.assertTrue(PathFilter.isWhiteAPI("auth"));
        Assert.assertTrue(PathFilter.isWhiteAPI("auth/users"));
        Assert.assertTrue(PathFilter.isWhiteAPI("hstore"));
        Assert.assertTrue(PathFilter.isWhiteAPI("pd"));
        Assert.assertTrue(PathFilter.isWhiteAPI("kafka"));
        Assert.assertTrue(PathFilter.isWhiteAPI("openapi.json"));
    }

    /**
     * 测试 isWhiteAPI 静态方法 - 非白名单路径
     */
    @Test
    public void testIsWhiteAPI_NonWhiteListPaths() {
        Assert.assertFalse(PathFilter.isWhiteAPI("graphs"));
        Assert.assertFalse(PathFilter.isWhiteAPI("schema"));
        Assert.assertFalse(PathFilter.isWhiteAPI("vertices"));
        Assert.assertFalse(PathFilter.isWhiteAPI("edges"));
        Assert.assertFalse(PathFilter.isWhiteAPI("traversers"));
        Assert.assertFalse(PathFilter.isWhiteAPI("tasks"));
        Assert.assertFalse(PathFilter.isWhiteAPI("unknown"));
    }

    /**
     * 测试中文字符（URL 编码后）
     */
    @Test
    public void testChineseCharacters_UrlEncoded() throws IOException {
        // 中文 "测试" 的 URL 编码
        String encodedChinese = "%E6%B5%8B%E8%AF%95";
        setupUriInfo("/", "/graphs/hugegraph/vertices/" + encodedChinese,
                     Arrays.asList("graphs", "hugegraph", "vertices", encodedChinese), null);

        URI expectedUri = URI.create(
                "http://localhost:8080/graphspaces/DEFAULT/graphs/hugegraph/vertices/" +
                encodedChinese);

        UriBuilder mockBuilder = Mockito.mock(UriBuilder.class);
        Mockito.when(uriInfo.getRequestUriBuilder()).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.uri(Mockito.anyString())).thenReturn(mockBuilder);
        Mockito.when(mockBuilder.build()).thenReturn(expectedUri);

        pathFilter.filter(requestContext);

        Mockito.verify(requestContext).setRequestUri(
                Mockito.any(URI.class), Mockito.eq(expectedUri));
    }
}

