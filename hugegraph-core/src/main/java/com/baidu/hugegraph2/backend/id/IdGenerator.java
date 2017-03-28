package com.baidu.hugegraph2.backend.id;

import com.baidu.hugegraph2.util.NumericUtil;
import com.baidu.hugegraph2.util.StringEncoding;

public class IdGenerator {

    /****************************** id type ******************************/

    public static enum IdType {
        LONG,
        STRING;
    }

    // this could be set by conf
    public static IdType ID_TYPE = IdType.STRING;

    /****************************** id generate ******************************/

    // generate a string id
    public static Id generate(String id) {
        switch (ID_TYPE) {
            case LONG:
                return new LongId(Long.parseLong(id));
            case STRING:
                return new StringId(id);
            default:
                assert false;
                return null;
        }
    }

    // generate a long id
    public static Id generate(long id) {
        switch (ID_TYPE) {
            case LONG:
                return new LongId(id);
            case STRING:
                return new StringId(String.valueOf(id));
            default:
                assert false;
                return null;
        }
    }

    // parse an id from bytes
    public static Id parse(byte[] bytes) {
        switch (ID_TYPE) {
            case LONG:
                return new LongId(bytes);
            case STRING:
                return new StringId(bytes);
            default:
                assert false;
                return null;
        }
    }

    /****************************** id defines ******************************/

    static class StringId implements Id {

        private String id;

        public StringId(String id) {
            this.id = id;
        }

        public StringId(byte[] bytes) {
            this.id = StringEncoding.decodeString(bytes);
        }

        @Override
        public String asString() {
            return this.id;
        }

        @Override
        public long asLong() {
            return Long.parseLong(this.id);
        }

        @Override
        public byte[] asBytes() {
            return StringEncoding.encodeString(this.id);
        }

        @Override
        public int compareTo(Id other) {
            return this.id.compareTo(((StringId) other).id);
        }

        @Override
        public String toString() {
            return this.asString();
        }
    }

    static class LongId implements Id {

        private long id;

        public LongId(long id) {
            this.id = id;
        }

        public LongId(byte[] bytes) {
            this.id = NumericUtil.bytesToLong(bytes);
        }

        @Override
        public String asString() {
            return String.valueOf(this.id);
        }

        @Override
        public long asLong() {
            return this.id;
        }

        @Override
        public byte[] asBytes() {
            return NumericUtil.longToBytes(this.id);
        }

        @Override
        public int compareTo(Id other) {
            long otherId = ((LongId) other).id;
            return Long.compare(this.id, otherId);
        }

        @Override
        public String toString() {
            return this.asString();
        }
    }
}
