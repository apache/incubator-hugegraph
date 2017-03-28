package com.baidu.hugegraph2.example;

import org.apache.tinkerpop.gremlin.structure.T;
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
public class ExampleGraphFactory {

    private static final Logger logger = LoggerFactory.getLogger(ExampleGraphFactory.class);

    public static void main(String[] args) {

        logger.info("ExampleGraphFactory start!");

        String confFile = ExampleGraphFactory.class.getClassLoader().getResource("hugegraph.properties").getPath();
        HugeGraph graph = HugeFactory.open(confFile);

        ExampleGraphFactory.showFeatures(graph);
        ExampleGraphFactory.load(graph);
    }

    public static void showFeatures(final HugeGraph graph) {
        logger.info("supportsPersistence : " + graph.features().graph().supportsPersistence());
    }

    public static void load(final HugeGraph graph) {

        /************************* schema operating *************************/
        SchemaManager schema = graph.openSchemaManager();
        logger.info("===============  propertyKey  ================");
        schema.propertyKey("name").asText().create();
        schema.propertyKey("gender").asText().create();
        schema.propertyKey("instructions").asText().create();
        schema.propertyKey("category").asText().create();
        schema.propertyKey("year").asInt().create();
        schema.propertyKey("timestamp").asTimestamp().create();
        schema.propertyKey("ISBN").asText().create();
        schema.propertyKey("calories").asInt().create();
        schema.propertyKey("amount").asText().create();
        schema.propertyKey("stars").asInt().create();
        schema.propertyKey("comment").asText().single().create();
        schema.propertyKey("nickname").asText().multiple().create();
        schema.propertyKey("lived").asText().create();
        schema.propertyKey("country").asText().multiple().properties("livedIn").create();
        schema.propertyKey("city_id").asInt().create();
        schema.propertyKey("sensor_id").asUuid().create();

        logger.info("===============  vertexLabel  ================");

        schema.vertexLabel("author").properties("name").create();
        schema.vertexLabel("recipe").properties("name", "instructions").create();
        schema.vertexLabel("ingredient").create();
        schema.vertexLabel("book").create();
        schema.vertexLabel("meal").create();
        schema.vertexLabel("reviewer").create();
        // vertex label must have the properties that specified in primary key
        schema.vertexLabel("FridgeSensor").properties("city_id").primaryKeys("city_id").create();

        logger.info("===============  vertexLabel & index  ================");
        // TODO: implement index feature.
        // schema.vertexLabel("author").index("byName").secondary().by("name").add();
        // schema.vertexLabel("recipe").index("byRecipe").materialized().by("name").add();
        // schema.vertexLabel("meal").index("byMeal").materialized().by("name").add();
        // schema.vertexLabel("ingredient").index("byIngredient").materialized().by("name").add();
        // schema.vertexLabel("reviewer").index("byReviewer").materialized().by("name").add();

        logger.info("===============  edgeLabel  ================");

        schema.edgeLabel("authored").linkOne2One().properties("contribution").sortKeys("contribution").create();
        schema.edgeLabel("created").single().linkMany2Many().create();
        schema.edgeLabel("includes").single().linkOne2Many().create();
        schema.edgeLabel("includedIn").linkMany2One().create();
        schema.edgeLabel("rated").multiple().linkMany2Many().link("reviewer", "recipe").create();

        // commit schema changes
//        schema.commit();

        logger.info("===============  schema desc  ================");
        schema.desc();

        /************************* data operating *************************/

        GraphTransaction tx = graph.openGraphTransaction();

        logger.info("===============  addVertex  ================");
        tx.addVertex(T.label, "book", "name", "java-1");
        tx.addVertex(T.label, "book", "name", "java-2");

        // commit data changes
        try {
            tx.commit();
        } catch (BackendException e) {
            e.printStackTrace();
            try {
                tx.rollback();
            } catch (BackendException e2) {
                // TODO Auto-generated catch block
                e2.printStackTrace();
            }
        }

        // use the default Transaction to commit
        graph.addVertex(T.label, "book", "name", "java-3");
        graph.tx().commit();
    }
}
