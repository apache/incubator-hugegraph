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
        ExampleGraphFactory.showFeatures(graph);
        ExampleGraphFactory.load(graph);
    }

    public static void showFeatures(final HugeGraph graph) {
        logger.info("supportsPersistence : " + graph.features().graph().supportsPersistence());
    }

    public static void load(final HugeGraph graph) {

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
        schema.vertexLabel("recipe").properties("name", "instructions").add();
        schema.vertexLabel("ingredient").create();
        schema.vertexLabel("book").create();
        schema.vertexLabel("meal").create();
        schema.vertexLabel("reviewer").create();
        schema.propertyKey("city_id").asInt().create();
        schema.propertyKey("sensor_id").asUUID().create();
        schema.vertexLabel("FridgeSensor").partitionKey("city_id").clusteringKey("sensor_id").create();

        System.out.println("===============  vertexLabel & index  ================");
        // index 表示要添加一个索引，secondary表示要添加的是二级索引，by指定了给哪一列添加索引
        schema.vertexLabel("author").index("byName").secondary().by("name").add();
        schema.vertexLabel("recipe").index("byRecipe").materialized().by("name").add();
        schema.vertexLabel("meal").index("byMeal").materialized().by("name").add();
        schema.vertexLabel("ingredient").index("byIngredient").materialized().by("name").add();
        schema.vertexLabel("reviewer").index("byReviewer").materialized().by("name").add();

        System.out.println("===============  edgeLabel  ================");
        schema.edgeLabel("authored").inOne2Many().create();
        schema.edgeLabel("created").single().inMany2Many().create();
        schema.edgeLabel("includes").single().inOne2Many().create();
        schema.edgeLabel("includedIn").inMany2One().create();
        schema.edgeLabel("rated").multiple().inMany2Many().connection("reviewer", "recipe").create();

        // Transaction tx = graph.openTX();

    }
}
