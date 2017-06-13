package com.baidu.hugegraph.example;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.VertexLabel;

public class PerfExample1 {

    public static final int PERSON_NUM = 70;
    public static final int SOFTWARE_NUM = 30;
    public static final int EDGE_NUM = 1000;

    private static final Logger logger = LoggerFactory.getLogger(PerfExample1.class);

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.out.println("Usage: times threadno");
        }

        int times = Integer.parseInt(args[1]);
        int threadno = Integer.parseInt(args[2]);;

        // NOTE: this test with HugeGraph is for local, change it into
        // client if test with restful server from remote
        HugeGraph hugegraph = ExampleUtil.loadGraph();
        GraphManager graph = new GraphManager(hugegraph);

        initSchema(hugegraph.schema());
        testInsertPerf(graph, times, threadno);

        System.exit(0);
    }

    public static void testInsertPerf(GraphManager graph,
            int times, int threadno) throws InterruptedException {
        List<Long> rates = new LinkedList<>();

        List<Thread> threads = new LinkedList<>();
        for (int i = 0; i < threadno; i++) {
            Thread t = new Thread(() -> {
                long rate = testInsertPerf(graph, times);
                rates.add(rate);
            });
            t.start();
            threads.add(t);
        }

        for (Thread t : threads) {
            t.join();
        }

        logger.info("Rate(virtual) with threads: {}",
                rates.stream().mapToLong(i -> i).sum());
    }

    public static void initSchema(SchemaManager schema) {
        schema.makePropertyKey("name").asText().create();
        schema.makePropertyKey("age").asInt().create();
        schema.makePropertyKey("lang").asText().create();
        schema.makePropertyKey("date").asText().create();
        schema.makePropertyKey("price").asInt().create();

        VertexLabel person = schema.makeVertexLabel("person")
                .properties("name", "age")
                .primaryKeys("name")
                .ifNotExist()
                .create();

        VertexLabel software = schema.makeVertexLabel("software")
                .properties("name", "lang", "price")
                .primaryKeys("name")
                .ifNotExist()
                .create();

//        schema.makeIndexLabel("personByName")
//                .on(person).by("name")
//                .secondary()
//                .ifNotExist()
//                .create();
//
//        schema.makeIndexLabel("softwareByPrice")
//                .on(software).by("price")
//                .search()
//                .ifNotExist()
//                .create();

        EdgeLabel knows = schema.makeEdgeLabel("knows")
                .link("person", "person")
                .properties("date")
                .ifNotExist()
                .create();

        EdgeLabel created = schema.makeEdgeLabel("created")
                .link("person", "software")
                .properties("date")
                .ifNotExist()
                .create();
    }

    public static long testInsertPerf(GraphManager graph, int times) {
        long total = EDGE_NUM * times;
        long startTime = System.currentTimeMillis();

        List<Object> personVertexIds = new ArrayList<>();
        List<Object> softwareVertexIds = new ArrayList<>();
        Random random = new Random();

        long startTime0, endTime0 = 0;
        while (times > 0) {
            startTime0 = System.currentTimeMillis();
            int personAge = 0;
            String personName = "";
            logger.debug("==============random person vertex===============");
            for (int i = 0; i < PERSON_NUM; i++) {
                random = new Random();
                personAge = random.nextInt(70);
                personName = "P" + random.nextInt(10000);
                Vertex vetex = graph.addVertex(T.label, "person",
                        "name", personName, "age", personAge);
                personVertexIds.add(vetex.id());
                logger.debug(vetex.toString());
            }

            int softwarePrice = 0;
            String softwareName = "";
            String softwareLang = "java";
            logger.debug("==============random software vertex============");
            for (int i = 0; i < SOFTWARE_NUM; i++) {
                random = new Random();
                softwarePrice = random.nextInt(10000) + 1;
                softwareName = "S" + random.nextInt(10000);
                Vertex vetex = graph.addVertex(T.label, "software",
                        "name", softwareName, "lang", "java",
                        "price", softwarePrice);
                softwareVertexIds.add(vetex.id());
            }

            // Random 1000 Edge
            logger.debug("====================add Edges=================");
            for (int i = 0; i < EDGE_NUM / 2; i++) {
                random = new Random();

                // Add edge: person --knows-> person
                Object p1 = personVertexIds.get(random.nextInt(PERSON_NUM));
                Object p2 = personVertexIds.get(random.nextInt(PERSON_NUM));
                Edge edge1 = graph.getVertex(p1).addEdge("knows",
                        graph.getVertex(p2));

                // Add edge: person --created-> software
                Object p3 = personVertexIds.get(random.nextInt(PERSON_NUM));
                Object s1 = softwareVertexIds.get(random.nextInt(SOFTWARE_NUM));
                Edge edge2 = graph.getVertex(p3).addEdge("created",
                        graph.getVertex(s1));
            }

            personVertexIds.clear();
            softwareVertexIds.clear();
            times--;
            endTime0 = System.currentTimeMillis();
            logger.debug("Adding edges during time: {} ms",
                    endTime0 - startTime0);
        }
        long endTime = System.currentTimeMillis();

        long cost = endTime - startTime;
        long rate = total * 1000 / cost;
        logger.info("All tests cost time: {} ms, the rate is: {} edges/s",
                cost, rate);
        return rate;
    }

    static class GraphManager {
        private HugeGraph hugegraph;

        public GraphManager(HugeGraph hugegraph) {
            this.hugegraph = hugegraph;
        }

        public Vertex addVertex(Object... keyValues) {
            return this.hugegraph.addVertex(keyValues);
        }

        public Vertex getVertex(Object id) {
            return this.hugegraph.vertices(id).next();
        }
    }
}


