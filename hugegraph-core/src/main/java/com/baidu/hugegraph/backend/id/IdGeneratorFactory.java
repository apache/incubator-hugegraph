package com.baidu.hugegraph.backend.id;

public class IdGeneratorFactory {

    // The default IdGenerator is SplicingIdGenerator
    private static IdGenerator initedGenerator = initGenerator("splicing");

    public static IdGenerator initGenerator(String generatorType) {
        initedGenerator = IdGeneratorFactory.createGenerator(generatorType);
        return initedGenerator;
    }

    public static IdGenerator generator() {
        return initedGenerator;
    }

    public static boolean supportSplicing() {
        return (initedGenerator instanceof SplicingIdGenerator);
    }

    private static IdGenerator createGenerator(String generatorType) {
        if (generatorType.equalsIgnoreCase("splicing")) {
            return new SplicingIdGenerator();
        } else if (generatorType.equalsIgnoreCase("snowflake")) {
            // TODO: read from conf
            long workerId = 0;
            long datacenterId = 0;
            return new SnowflakeIdGenerator(workerId, datacenterId);
        }

        // Add others IdGenerator here

        return null;
    }
}
