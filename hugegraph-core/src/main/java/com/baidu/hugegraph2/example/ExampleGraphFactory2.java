package com.baidu.hugegraph2.example;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.HugeFactory;
import com.baidu.hugegraph2.HugeGraph;
import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.tx.GraphTransaction;
import com.baidu.hugegraph2.schema.SchemaManager;

/**
 * Created by jishilei on 17/3/16.
 */
public class ExampleGraphFactory2 {

    private static final Logger logger = LoggerFactory.getLogger(ExampleGraphFactory2.class);

    public static void main(String[] args) {

        logger.info("ExampleGraphFactory start!");

        String confFile = ExampleGraphFactory2.class.getClassLoader().getResource("hugegraph.properties").getPath();
        HugeGraph graph = HugeFactory.open(confFile);

        ExampleGraphFactory2.showFeatures(graph);
        ExampleGraphFactory2.load(graph);
    }

    public static void showFeatures(final HugeGraph graph) {
        logger.info("supportsPersistence : " + graph.features().graph().supportsPersistence());
    }

    public static void load(final HugeGraph graph) {

        /************************* schemaManager operating *************************/
        SchemaManager schemaManager = graph.openSchemaManager();
        logger.info("===============  propertyKey  ================");
        schemaManager.propertyKey("name").asText().create();
        schemaManager.propertyKey("contribution").asText().create();

        logger.info("===============  vertexLabel  ================");
        schemaManager.vertexLabel("author").properties("name").primaryKeys("name").create();
        schemaManager.vertexLabel("book").properties("name").primaryKeys("name").create();

        logger.info("===============  edgeLabel  ================");

        schemaManager.edgeLabel("authored").multiTimes().linkOne2One().properties("time").sortKeys("time")
                .create();

        logger.info("===============  schemaManager desc  ================");
        schemaManager.desc();

        /************************* data operating *************************/

        // Must commit manually
        GraphTransaction tx = graph.openGraphTransaction();

        logger.info("===============  addVertex  ================");
        Vertex person = tx.addVertex(T.label, "author", "name", "James Gosling", "age", "60");

        Vertex book1 = tx.addVertex(T.label, "book", "name", "java-1");
        Vertex book2 = tx.addVertex(T.label, "book", "name", "java-2");

        person.addEdge("authored", book1, "time", "1990-1-1");
        person.addEdge("authored", book2, "time", "2017-4-28");

        // commit data changes
        try {
            tx.commit();
        } catch (BackendException e) {
            logger.error(e.getMessage());
            try {
                tx.rollback();
            } catch (BackendException e2) {
                // TODO Auto-generated catch block
                e2.printStackTrace();
            }
        }

    }
}
