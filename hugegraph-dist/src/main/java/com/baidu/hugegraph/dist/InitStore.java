package com.baidu.hugegraph.dist;

import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.tree.ConfigurationNode;
import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration;

import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.util.E;

/**
 * Created by liningrui on 2017/6/19.
 */
public class InitStore {

    public static void main(String[] args)
                  throws ConfigurationException, InterruptedException {

        E.checkArgument(args.length == 1,
                        "Init store only accept one config file.");
        E.checkArgument(args[0].endsWith(".yaml"),
                        "Init store only accept yaml config file.");

        String confFile = args[0];
        RegisterUtil.registerBackends();

        YamlConfiguration config = new YamlConfiguration();
        config.load(confFile);

        List<ConfigurationNode> graphNames = config.getRootNode().getChildren(
                CoreOptions.GRAPHS.name()).get(0).getChildren();

        E.checkArgumentNotNull(graphNames,
                               "Not found the node '%s' in the config file %s",
                               CoreOptions.GRAPHS.name(), confFile);
        E.checkArgument(!graphNames.isEmpty(),
                        "The node '%s' must contain at least one child node");

        for (ConfigurationNode graphName : graphNames) {
            String graphPropFile = graphName.getValue().toString();
            // get graph property file path
            HugeGraph graph = HugeFactory.open(graphPropFile);
            graph.clearBackend();
            graph.initBackend();
            graph.close();
        }

        // Wait cache clear or init up to 30 seconds
        EventHub.destroy(30);
    }

}
