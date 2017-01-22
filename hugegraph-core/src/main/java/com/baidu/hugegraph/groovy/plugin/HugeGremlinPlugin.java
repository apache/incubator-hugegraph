/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.groovy.plugin;

import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;

import com.baidu.hugegraph.structure.HugeGraph;

/**
 * Created by zhangsuochao on 17/2/7.
 */
public class HugeGremlinPlugin extends AbstractGremlinPlugin {
    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + HugeGraph.class.getPackage().getName() + DOT_STAR);
    }};

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor)
            throws PluginInitializationException, IllegalEnvironmentException {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public void afterPluginTo(PluginAcceptor pluginAcceptor)
            throws IllegalEnvironmentException, PluginInitializationException {

    }

    @Override
    public String getName() {
        return "baidu.hugegraph";
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}
