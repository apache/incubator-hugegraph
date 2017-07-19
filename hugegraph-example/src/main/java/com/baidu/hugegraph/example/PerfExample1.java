package com.baidu.hugegraph.example;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.cache.Cache;
import com.baidu.hugegraph.backend.cache.RamCache;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeVertex;
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
            return;
        }

        int times = Integer.parseInt(args[1]);
        int threadno = Integer.parseInt(args[2]);;

        // NOTE: this test with HugeGraph is for local, change it into
        // client if test with restful server from remote
        HugeGraph hugegraph = ExampleUtil.loadGraph();
        GraphManager graph = new GraphManager(hugegraph);

        initSchema(hugegraph.schema());
        testInsertPerf(graph, times, threadno);

        hugegraph.close();
        System.exit(0);
    }

    public static void testInsertPerf(GraphManager graph,
            int times, int threadno) throws InterruptedException {
        List<Pair<Long, Long>> rates = new LinkedList<>();

        List<Thread> threads = new LinkedList<>();
        for (int i = 0; i < threadno; i++) {
            Thread t = new Thread(() -> {
                graph.tx().open();
                Pair<Long, Long> rate = testInsertPerf(graph, times);
                graph.tx().close();

                rates.add(rate);
            });
            t.start();
            threads.add(t);
        }

        for (Thread t : threads) {
            t.join();
        }

        // total edges
        long edges = rates.stream().mapToLong(i -> i.getLeft()).sum();
        // total cost (average time of all threads) (ms)
        long cost = (long) rates.stream().mapToLong(i -> i.getRight())
                    .average().getAsDouble();
        logger.info("Rate with threads: {} edges/s", edges * 1000 / cost);
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

    public static Pair<Long, Long> testInsertPerf(GraphManager graph,
                                                  int times) {
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
                logger.debug("Add vertex: {}", vetex);
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

            graph.tx().commit();
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
        return Pair.of(total, cost);
    }

    static class GraphManager {
        private HugeGraph hugegraph;
        private Cache cache = new RamCache();

        public GraphManager(HugeGraph hugegraph) {
            this.hugegraph = hugegraph;
        }

        public Transaction tx() {
            return this.hugegraph.tx();
        }

        public Vertex addVertex(Object... keyValues) {
            Vertex v = this.hugegraph.addVertex(keyValues);
            this.cache.update((Id) v.id(), v);
            return v;
        }

        public Vertex getVertex(Object id) {
            return ((HugeVertex) this.cache.getOrFetch((Id) id, k -> {
                return this.hugegraph.vertices(k).next();
            })).copy();
        }
    }
}


