package com.baidu.hugegraph2.plugin;

import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;

/**
 * Created by liningrui on 2017/3/27.
 */
public class HugeGraphGremlinPlugin implements GremlinPlugin {

    private static final String IMPORT = "import ";
    private static final String IMPORT_STATIC = IMPORT + "static ";
    private static final String DOT_STAR = ".*";

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT + "com.baidu.hugegraph2" + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "hugegraph2.imports";
    }


    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}
