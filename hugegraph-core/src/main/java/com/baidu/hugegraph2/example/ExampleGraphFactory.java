package com.baidu.hugegraph2.example;

import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.HugeFactory;
import com.baidu.hugegraph2.HugeGraph;
import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.tx.GraphTransaction;
import com.baidu.hugegraph2.configuration.HugeConfiguration;
import com.baidu.hugegraph2.schema.SchemaManager;

/**
 * Created by jishilei on 17/3/16.
 */
public class ExampleGraphFactory {

    private static final Logger logger = LoggerFactory.getLogger(ExampleGraphFactory.class);

    public static void main(String[] args) {

        logger.info("ExampleGraphFactory start!");

        HugeConfiguration configuration = new HugeConfiguration()
                .useMemoryBackend();
        HugeGraph graph = HugeFactory.open(configuration);

        ExampleGraphFactory.showFeatures(graph);

        ExampleGraphFactory.load(graph);
    }

    public static void showFeatures(final HugeGraph graph) {
        logger.info("supportsPersistence : " + graph.features().graph().supportsPersistence());
    }

    public static void load(final HugeGraph graph) {

        /************************* schema operating *************************/

        SchemaManager schema = graph.openSchemaManager();
        System.out.println("===============  propertyKey  ================");
        // 设置属性的schema
        schema.propertyKey("name").asText().create();
        schema.propertyKey("gender").asText().create();
        schema.propertyKey("instructions").asText().create();
        schema.propertyKey("category").asText().create();
        schema.propertyKey("year").asInt().create();
        schema.propertyKey("timestamp").asTimeStamp().create();
        schema.propertyKey("ISBN").asText().create();
        schema.propertyKey("calories").asInt().create();
        schema.propertyKey("amount").asText().create();
        schema.propertyKey("stars").asInt().create();
        schema.propertyKey("comment").asText().single().create();
        schema.propertyKey("nickname").asText().multiple().create();
        schema.propertyKey("lived").asText().create();
        // 给property设置property
        schema.propertyKey("country").asText().multiple().properties("livedIn").create();

        System.out.println("===============  vertexLabel  ================");

        // 设置顶点的schema
        schema.vertexLabel("author").properties("name").create();
        schema.vertexLabel("recipe").properties("name", "instructions").create();
//        schema.vertexLabel("recipe").properties("name", "instructions").add();
        schema.vertexLabel("ingredient").create();
        schema.vertexLabel("book").create();
        schema.vertexLabel("meal").create();
        schema.vertexLabel("reviewer").create();

        schema.propertyKey("city_id").asInt().create();
        schema.propertyKey("sensor_id").asUUID().create();
        schema.vertexLabel("FridgeSensor").partitionKey("city_id").clusteringKey("sensor_id").create();

        System.out.println("===============  vertexLabel & index  ================");
        // index 表示要添加一个索引，secondary表示要添加的是二级索引，by指定了给哪一列添加索引
//        schema.vertexLabel("author").index("byName").secondary().by("name").add();
//        schema.vertexLabel("recipe").index("byRecipe").materialized().by("name").add();
        // TODO: fix these errors!
        // schema.vertexLabel("meal").index("byMeal").materialized().by("name").add();
        // schema.vertexLabel("ingredient").index("byIngredient").materialized().by("name").add();
        // schema.vertexLabel("reviewer").index("byReviewer").materialized().by("name").add();

        System.out.println("===============  edgeLabel  ================");

        schema.edgeLabel("authored").linkOne2One().create();
        schema.edgeLabel("created").single().linkMany2Many().create();
        schema.edgeLabel("includes").single().linkOne2Many().create();
        schema.edgeLabel("includedIn").linkMany2One().create();
        schema.edgeLabel("rated").multiple().linkMany2Many().link("reviewer", "recipe").create();

        // commit schema changes
        schema.commit();

        schema.desc();

        /************************* data operating *************************/

        GraphTransaction tx = graph.openGraphTransaction();

        System.out.println("===============  addVertex  ================");
        tx.addVertex(T.id, "1", T.label, "book", "name", "java-1");
        tx.addVertex(T.id, "2", T.label, "book", "name", "java-2");

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
        graph.addVertex(T.id, "3", T.label, "book", "name", "java-3");
        graph.tx().commit();
    }
}
