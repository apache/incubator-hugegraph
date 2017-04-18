package com.baidu.hugegraph.backend.id;

public class IdGeneratorFactory {

    // the default IdGenerator is SplicingIdGenerator
    private static IdGenerator initedGenerator = initGenerator("splicing");

    public static IdGenerator initGenerator(String generatorType) {
        initedGenerator = IdGeneratorFactory.createGenerator(generatorType);
        return initedGenerator;
    }

    public static IdGenerator generator() {
        return initedGenerator;
    }

    private static IdGenerator createGenerator(String generatorType) {
        if (generatorType.equalsIgnoreCase("splicing")) {
            return new SplicingIdGenerator();
        }
        else if (generatorType.equalsIgnoreCase("snowflake")) {
            // TODO: read from conf
            long workerId = 0;
            long datacenterId = 0;
            return new SnowflakeIdGenerator(workerId, datacenterId);
        }
        else if (generatorType.equalsIgnoreCase("bytes-splicing")) {
            // return new BytesSplicingIdGenerator();
            return null;
        }

        // add others IdGenerator here

        return null;
    }
}
