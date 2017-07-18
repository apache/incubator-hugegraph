package com.baidu.hugegraph.backend.id;

import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.NumericUtil;
import com.baidu.hugegraph.util.StringEncoding;

public abstract class IdGenerator {

    /****************************** id type **********************************/

    public static enum IdType {
        LONG,
        STRING;
    }

    // This could be set by conf
    public static IdType ID_TYPE = IdType.STRING;

    /****************************** id generate ******************************/

    public abstract Id generate(SchemaElement entry);

    public abstract Id generate(HugeVertex entry);

    public abstract Id generate(HugeEdge entry);

    /**
     * Generate a string id
     */
    public Id generate(String id) {
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

    /**
     * Generate a long id
     */
    public Id generate(long id) {
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

    /**
     * Parse an id from bytes
     */
    public Id parse(byte[] bytes) {
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

    public static class StringId implements Id {

        private String id;

        public StringId(String id) {
            this.id = id;
        }

        public StringId(byte[] bytes) {
            this.id = StringEncoding.decodeString(bytes);
        }

        @Override
        public Id prefixWith(HugeType type) {
            return new StringId(String.format("%x%s", type.code(), this.id));
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
        public int hashCode() {
            return this.id.hashCode();
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof StringId)) {
                return false;
            }
            return this.id.equals(((StringId) other).id);
        }

        @Override
        public String toString() {
            return this.asString();
        }
    }

    public static class LongId implements Id {

        private long id;

        public LongId(long id) {
            this.id = id;
        }

        public LongId(byte[] bytes) {
            this.id = NumericUtil.bytesToLong(bytes);
        }

        @Override
        public Id prefixWith(HugeType type) {
            long t = type.code();
            this.id = (this.id & 0x00ffffffffffffffL) & (t << 56);
            return this;
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
        public int hashCode() {
            return (int) this.id;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof LongId)) {
                return false;
            }
            return this.id == ((LongId) other).id;
        }

        @Override
        public String toString() {
            return this.asString();
        }
    }

}
