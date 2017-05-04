// an init script that returns a Map allows explicit setting of global bindings.
def globals = [:]

// defines a sample LifeCycleHook that prints some output to the Gremlin Server console.
// note that the name of the key in the "global" map is unimportant.
globals << [hook: [
        onStartUp : { ctx ->
            ctx.logger.info("Executed once at startup of Gremlin Server.")

            def schema = graph.openSchemaManager()

            schema.propertyKey("id").asInt().ifNotExits().create()
            schema.propertyKey("name").asText().ifNotExits().create()
            schema.propertyKey("age").asInt().ifNotExits().create()
            schema.propertyKey("date").asText().ifNotExits().create()
            schema.propertyKey("lang").asText().ifNotExits().create()
            schema.propertyKey("ISBN").asText().ifNotExits().create()
            schema.propertyKey("IDCardNO").asText().properties("length").ifNotExits().create()

            schema.vertexLabel("person").properties("IDCardNO", "name").primaryKeys("IDCardNO").ifNotExits().create()
            schema.vertexLabel("book").properties("name", "ISBN").primaryKeys("ISBN").ifNotExits().create()
            schema.vertexLabel("person").index("byName").by("name").secondary().ifNotExits().create()
            schema.vertexLabel("book").index("byName").by("name").secondary().ifNotExits().create()
            schema.vertexLabel("software").properties("name").primaryKeys("name").ifNotExits().create()

            schema.edgeLabel("authored").single().connection("person", "book").properties("date").ifNotExits().create()
            schema.edgeLabel("knows").single().connection("person", "person").properties("date").ifNotExits().create()
            schema.edgeLabel("created").single().connection("person", "software").properties("date").ifNotExits().create()

            def person = graphManager.addVertex(T.label, "person", "IDCardNO", "410212194009323410", "name", "James " +
                    "Gosling", "age", "60", "lived", "")
            def hadoop = graphManager.addVertex(T.label, "book", "ISBN", "123456", "name", "hadoop in action")
            def java = graphManager.addVertex(T.label, "book", "ISBN", "3456790", "name", "Java Study")
            person.addEdge("authored", hadoop, "date", "2015-06-01")
            person.addEdge("authored", java, "date", "2017-02-14")

            def marko = graphManager.addVertex(T.label, "person", "name", "marko", "IDCardNO", "530923198205064395", "age", 29)
            def vadas = graphManager.addVertex(T.label, "person", "name", "vadas", "IDCardNO", "140928198011228979", "age", 27)
            def lop = graphManager.addVertex(T.label, "software", "name", "lop", "lang", "java")
            def josh = graphManager.addVertex(T.label, "person", "name", "josh", "IDCardNO", "341003198202061210", "age", 32)
            def ripple = graphManager.addVertex(T.label, "software", "name", "ripple", "lang", "java")
            def peter = graphManager.addVertex(T.label, "person", "name", "peter", "IDCardNO", "230505198707253596", "age", 35)

            marko.addEdge("knows", vadas, "date", "2012-12-12")
            marko.addEdge("knows", josh, "date", "2008-10-01")
            marko.addEdge("created", lop, "date", "2009-03-27")
            josh.addEdge("created", ripple, "date", "1946-12-04")
            josh.addEdge("created", lop, "date", "2001-04-01")
            peter.addEdge("created", lop, "date", "1998-12-28")
        },
        onShutDown: { ctx ->
            ctx.logger.info("Executed once at shutdown of Gremlin Server.")
        }
] as LifeCycleHook]

// define the default TraversalSource to bind queries to - this one will be named "g".
// globals << [schemaManager: graph.openSchemaManager()]
globals << [graphManager: graph.openGraphManager()]
globals << [schema: graph.openSchemaManager()]
globals << [g: graph.traversal()]
