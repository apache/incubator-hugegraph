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

package org.apache.hugegraph.auth;

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.config.ConfigException;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.rpc.RpcClientProviderWithAuth;
import org.apache.hugegraph.util.ConfigUtil;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.StringEncoding;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import jakarta.ws.rs.core.SecurityContext;

public class StandardAuthenticator implements HugeAuthenticator {

    private static final String INITING_STORE = "initing_store";
    private static final int MAX_ADMIN_PA_FILE_BYTES = 4096;
    private static final int MAX_ADMIN_PASSWORD_BYTES = 72;

    private HugeGraph graph = null;

    private void initAdminUser(HugeConfig config) throws Exception {
        // init-store only closes the graphs it initialized and skips hstore
        // ones, so close the auth graph here even when the password file is
        // refused, without letting a close failure hide that error
        try {
            if (this.requireInitAdminUser()) {
                String file = config.get(ServerOptions.ADMIN_PA_FILE);
                this.initAdminUser(file.isEmpty() ? this.inputPassword() :
                                   readAdminPasswordFile(file));
            }
        } catch (Throwable e) {
            try {
                this.graph.close();
            } catch (Throwable closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
        this.graph.close();
    }

    @Override
    public HugeGraph graph() {
        E.checkState(this.graph != null, "Must setup Authenticator first");
        return this.graph;
    }

    @Override
    public void initAdminUser(String password) {
        // Not allowed to call by non-main thread
        String caller = Thread.currentThread().getName();
        E.checkState("main".equals(caller), "Invalid caller '%s'", caller);

        AuthManager authManager = this.graph().hugegraph().authManager();
        // Only init user when local mode and user has not been initialized
        if (this.requireInitAdminUser()) {
            HugeUser admin = new HugeUser(HugeAuthenticator.USER_ADMIN);
            admin.password(StringEncoding.hashPassword(password));
            admin.creator(HugeAuthenticator.USER_SYSTEM);
            authManager.createUser(admin);
        }
    }

    private boolean requireInitAdminUser() {
        AuthManager authManager = this.graph().hugegraph().authManager();
        return StandardAuthManager.isLocal(authManager) &&
               authManager.findUser(HugeAuthenticator.USER_ADMIN) == null;
    }

    private String inputPassword() {
        String inputPrompt = "Please input the admin password:";
        String notEmptyPrompt = "The admin password can't be empty";
        Console console = System.console();
        while (true) {
            String password;
            if (console != null) {
                char[] chars = console.readPassword(inputPrompt);
                password = new String(chars);
            } else {
                // CHECKSTYLE:OFF
                System.out.println(inputPrompt);
                // CHECKSTYLE:ON
                // just wrapper of System.in
                Scanner scanner = new Scanner(System.in);
                password = scanner.nextLine();
            }
            if (!password.isEmpty()) {
                return password;
            }
            // CHECKSTYLE:OFF
            System.out.println(notEmptyPrompt);
            // CHECKSTYLE:ON
        }
    }

    @Override
    public void setup(HugeConfig config) {
        String graphName = config.get(ServerOptions.AUTH_GRAPH_STORE);
        Map<String, String> graphConfs = ConfigUtil.scanGraphsDir(
                config.get(ServerOptions.GRAPHS));
        String graphPath = graphConfs.get(graphName);
        E.checkArgument(graphPath != null,
                        "Can't find graph name '%s' in config '%s' at " +
                        "'rest-server.properties' to store auth information, " +
                        "please ensure the value of '%s' matches it correctly",
                        graphName, ServerOptions.GRAPHS,
                        ServerOptions.AUTH_GRAPH_STORE.name());

        HugeConfig graphConfig = new HugeConfig(graphPath);
        if (config.getProperty(INITING_STORE) != null &&
            config.getBoolean(INITING_STORE)) {
            // Forced set RAFT_MODE to false when initializing backend
            graphConfig.setProperty(CoreOptions.RAFT_MODE.name(), "false");
        }

        // Transfer `raft.group_peers` from server config to graph config
        String raftGroupPeers = config.get(ServerOptions.RAFT_GROUP_PEERS);
        graphConfig.addProperty(ServerOptions.RAFT_GROUP_PEERS.name(),
                                raftGroupPeers);

        this.graph = (HugeGraph) GraphFactory.open(graphConfig);

        String remoteUrl = config.get(ServerOptions.AUTH_REMOTE_URL);
        if (StringUtils.isNotEmpty(remoteUrl)) {
            RpcClientProviderWithAuth clientProvider =
                    new RpcClientProviderWithAuth(config);
            this.graph.switchAuthManager(clientProvider.authManager());
        }
    }

    /**
     * Verify if a user is legal
     *
     * @param username the username for authentication
     * @param password the password for authentication
     * @param token    the token for authentication
     * @return String No permission if return ROLE_NONE else return a role
     */
    @Override
    public UserWithRole authenticate(String username, String password,
                                     String token) {
        UserWithRole userWithRole;
        if (StringUtils.isNotEmpty(token)) {
            userWithRole = this.authManager().validateUser(token);
        } else {
            E.checkArgumentNotNull(username,
                                   "The username parameter can't be null");
            E.checkArgumentNotNull(password,
                                   "The password parameter can't be null");
            userWithRole = this.authManager().validateUser(username, password);
        }

        RolePermission role = userWithRole.role();
        if (role == null) {
            role = ROLE_NONE;
        } else if (USER_ADMIN.equals(userWithRole.username())) {
            role = ROLE_ADMIN;
        } else {
            return userWithRole;
        }

        return new UserWithRole(userWithRole.userId(),
                                userWithRole.username(), role);
    }

    @Override
    public void unauthorize(SecurityContext context) {
        HugeGraphAuthProxy.resetContext();
    }

    @Override
    public AuthManager authManager() {
        return this.graph().authManager();
    }

    @Override
    public SaslNegotiator newSaslNegotiator(InetAddress remoteAddress) {
        return new TokenSaslAuthenticator();
    }

    public static void initAdminUserIfNeeded(String confFile) throws Exception {
        StandardAuthenticator auth = new StandardAuthenticator();
        HugeConfig config = new HugeConfig(confFile);
        String authClass = config.get(ServerOptions.AUTHENTICATOR);
        if (authClass.isEmpty()) {
            return;
        }
        config.addProperty(INITING_STORE, true);
        auth.setup(config);
        if (auth.graph().backendStoreFeatures().supportsPersistence()) {
            auth.initAdminUser(config);
        }
    }

    /**
     * The initial admin password for the bootstrap paths that cannot prompt.
     * auth.admin_pa goes through the properties grammar, which trims it,
     * processes backslash escapes and decodes it as ISO-8859-1, so
     * auth.admin_pa_file takes precedence when both are set.
     */
    public static String initialAdminPassword(HugeConfig config) {
        String file = config.get(ServerOptions.ADMIN_PA_FILE);
        if (!file.isEmpty()) {
            return readAdminPasswordFile(file);
        }
        return config.get(ServerOptions.ADMIN_PA);
    }

    private static String readAdminPasswordFile(String file) {
        String option = ServerOptions.ADMIN_PA_FILE.name();
        byte[] bytes;
        try {
            Path path = Paths.get(file);
            // Checked before opening, because opening a FIFO blocks until a
            // writer connects. Symlinks are followed, so a Kubernetes secret
            // mounted through ..data links still counts as a regular file
            if (!Files.readAttributes(path, BasicFileAttributes.class)
                      .isRegularFile()) {
                throw new ConfigException("The file '%s' set by '%s' is not a regular file",
                                          file, option);
            }
            // Bounded, so a path that points at a large file fails quickly
            try (InputStream in = Files.newInputStream(path)) {
                bytes = in.readNBytes(MAX_ADMIN_PA_FILE_BYTES + 1);
            }
        } catch (IOException | InvalidPathException e) {
            throw new ConfigException("Failed to read the file '%s' set by '%s' (%s)",
                                      e, file, option, e.getClass().getSimpleName());
        }
        if (bytes.length > MAX_ADMIN_PA_FILE_BYTES) {
            throw new ConfigException("The file '%s' set by '%s' is larger than %s bytes",
                                      file, option, MAX_ADMIN_PA_FILE_BYTES);
        }

        String password;
        try {
            password = StandardCharsets.UTF_8.newDecoder()
                                             .onMalformedInput(CodingErrorAction.REPORT)
                                             .onUnmappableCharacter(CodingErrorAction.REPORT)
                                             .decode(ByteBuffer.wrap(bytes))
                                             .toString();
        } catch (CharacterCodingException e) {
            throw new ConfigException("The file '%s' set by '%s' is not valid UTF-8",
                                      e, file, option);
        }

        // Some Windows editors and PowerShell prepend a BOM; it is never meant
        // as part of the password and would make the admin unreachable
        if (password.startsWith("\uFEFF")) {
            password = password.substring(1);
        }
        // Drop the line break that echo, editors and most secret tools append
        if (password.endsWith("\r\n")) {
            password = password.substring(0, password.length() - 2);
        } else if (password.endsWith("\n")) {
            password = password.substring(0, password.length() - 1);
        }
        if (password.isEmpty()) {
            throw new ConfigException("The file '%s' set by '%s' is empty",
                                      file, option);
        }
        // jBCrypt hashes only the first 72 bytes, so a longer password would
        // also accept anything that shares its first 72 bytes
        if (password.getBytes(StandardCharsets.UTF_8).length > MAX_ADMIN_PASSWORD_BYTES) {
            throw new ConfigException("The password in the file '%s' set by '%s' is " +
                                      "longer than %s bytes in UTF-8, the most that " +
                                      "the password hash uses",
                                      file, option, MAX_ADMIN_PASSWORD_BYTES);
        }
        return password;
    }

    private class TokenSaslAuthenticator implements SaslNegotiator {

        private static final byte NUL = 0;
        private String username;
        private String password;
        private String token;

        @Override
        public byte[] evaluateResponse(final byte[] clientResponse) throws AuthenticationException {
            decode(clientResponse);
            return null;
        }

        @Override
        public boolean isComplete() {
            return this.username != null;
        }

        @Override
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException {
            if (!this.isComplete()) {
                throw new AuthenticationException(
                        "The SASL negotiation has not yet been completed.");
            }

            final Map<String, String> credentials = new HashMap<>(6, 1);
            credentials.put(KEY_USERNAME, username);
            credentials.put(KEY_PASSWORD, password);
            credentials.put(KEY_TOKEN, token);

            return authenticate(credentials);
        }

        /**
         * SASL PLAIN mechanism specifies that credentials are encoded in a
         * sequence of UTF-8 bytes, delimited by 0 (US-ASCII NUL).
         * The form is : {code}authzId<NUL>authnId<NUL>password<NUL>{code}.
         *
         * @param bytes encoded credentials string sent by the client
         */
        private void decode(byte[] bytes) throws AuthenticationException {
            this.username = null;
            this.password = null;

            int end = bytes.length;

            for (int i = bytes.length - 1; i >= 0; i--) {
                if (bytes[i] != NUL) {
                    continue;
                }
                if (this.password == null) {
                    password = new String(Arrays.copyOfRange(bytes, i + 1, end),
                                          StandardCharsets.UTF_8);
                } else if (this.username == null) {
                    username = new String(Arrays.copyOfRange(bytes, i + 1, end),
                                          StandardCharsets.UTF_8);
                }
                end = i;
            }

            if (this.username == null) {
                throw new AuthenticationException("SASL authentication ID must not be null.");
            }
            if (this.password == null) {
                throw new AuthenticationException("SASL password must not be null.");
            }

            /* The trick is here. >_*/
            if (password.isEmpty()) {
                token = username;
            }
        }
    }
}
