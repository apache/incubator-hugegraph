package com.baidu.hugegraph2.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.HugeFactory;
import com.baidu.hugegraph2.schema.base.maker.SchemaManager;
import com.baidu.hugegraph2.structure.HugeGraph;

/**
 * Created by jishilei on 17/3/16.
 */
public class ExampleGraphFactory {

    private static final Logger logger = LoggerFactory.getLogger(ExampleGraphFactory.class);

    public static void main(String[] args) {

        logger.info("ExampleGraphFactory start!");

        HugeGraph graph = HugeFactory.open();

        ExampleGraphFactory.load(graph);

    }

    public static void load(final HugeGraph graph) {

        SchemaManager schema = graph.openSchemaManager();
        schema.propertyKey("id").toInt().create();
        schema.propertyKey("name").toText().create();
        schema.propertyKey("age").toInt().create();

        schema.propertyKey("age").remove();

        schema.desc();

        // Transaction tx = graph.openTX();



    }
}
