package com.baidu.hugegraph.plugin;

import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.groovy.plugin.GremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;

import com.baidu.hugegraph.io.HugeGraphIoRegistry;

/**
 * Created by liningrui on 2017/3/27.
 */
public class HugeGraphGremlinPlugin implements GremlinPlugin {

    private static final String IMPORT = "import ";
    private static final String IMPORT_STATIC = IMPORT + "static ";
    private static final String DOT_STAR = ".*";

    private static final Set<String> IMPORTS = new HashSet<String>() {
        {
            add(IMPORT + "com.baidu.hugegraph" + DOT_STAR);
            add(IMPORT + HugeGraphIoRegistry.class.getName());
        }
    };

    @Override
    public String getName() {
        return "com.baidu.hugegraph";
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
