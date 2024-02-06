/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: store_stream.proto

package org.apache.hugegraph.store.grpc.stream;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

/**
 * 为了提升性能，复用内存，减少gc回收，需要重写KvStream.writeTo方法
 */
public final class KvStream extends
                            com.google.protobuf.GeneratedMessageV3 implements
                                                                   // @@protoc_insertion_point
                                                                           // (message_implements
                                                                           // :KvStream)
                                                                           KvStreamOrBuilder {
    public static final int SEQ_NO_FIELD_NUMBER = 1;
    public static final int OVER_FIELD_NUMBER = 2;
    public static final int VERSION_FIELD_NUMBER = 4;
    public static final int STREAM_FIELD_NUMBER = 5;
    public static final int TYPE_FIELD_NUMBER = 6;
    private static final long serialVersionUID = 0L;
    // @@protoc_insertion_point(class_scope:KvStream)
    private static final KvStream DEFAULT_INSTANCE;
    private static final com.google.protobuf.Parser<KvStream>
            PARSER = new com.google.protobuf.AbstractParser<>() {
        @java.lang.Override
        public KvStream parsePartialFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws com.google.protobuf.InvalidProtocolBufferException {
            return new KvStream(input, extensionRegistry);
        }
    };

    static {
        DEFAULT_INSTANCE = new KvStream();
    }

    private int seqNo_;
    private boolean over_;
    private int version_;
    private ByteBuffer stream_;
    private Consumer<KvStream> complete_;
    private int type_;
    private byte memoizedIsInitialized = -1;

    // Use KvStream.newBuilder() to construct.
    private KvStream(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
        super(builder);
    }

    private KvStream() {
        stream_ = ByteBuffer.allocate(0);
    }
    private KvStream(
            com.google.protobuf.CodedInputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
        this();
        if (extensionRegistry == null) {
            throw new java.lang.NullPointerException();
        }
        com.google.protobuf.UnknownFieldSet.Builder unknownFields =
                com.google.protobuf.UnknownFieldSet.newBuilder();
        try {
            boolean done = false;
            while (!done) {
                int tag = input.readTag();
                switch (tag) {
                    case 0:
                        done = true;
                        break;
                    case 8: {

                        seqNo_ = input.readInt32();
                        break;
                    }
                    case 16: {

                        over_ = input.readBool();
                        break;
                    }
                    case 32: {

                        version_ = input.readUInt32();
                        break;
                    }
                    case 42: {

                        stream_ = input.readByteBuffer();
                        break;
                    }
                    default: {
                        if (!parseUnknownField(
                                input, unknownFields, extensionRegistry, tag)) {
                            done = true;
                        }
                        break;
                    }
                }
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw e.setUnfinishedMessage(this);
        } catch (java.io.IOException e) {
            throw new com.google.protobuf.InvalidProtocolBufferException(
                    e).setUnfinishedMessage(this);
        } finally {
            this.unknownFields = unknownFields.build();
            makeExtensionsImmutable();
        }
    }

    public static com.google.protobuf.Descriptors.Descriptor
    getDescriptor() {
        return HgStoreStreamProto.internal_static_KvStream_descriptor;
    }

    public static KvStream parseFrom(
            java.nio.ByteBuffer data)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data);
    }

    public static KvStream parseFrom(
            java.nio.ByteBuffer data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data, extensionRegistry);
    }

    public static KvStream parseFrom(
            com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data);
    }

    public static KvStream parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data, extensionRegistry);
    }

    public static KvStream parseFrom(byte[] data)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data);
    }

    public static KvStream parseFrom(
            byte[] data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data, extensionRegistry);
    }

    public static KvStream parseFrom(java.io.InputStream input)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseWithIOException(PARSER, input);
    }

    public static KvStream parseFrom(
            java.io.InputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static KvStream parseDelimitedFrom(java.io.InputStream input)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseDelimitedWithIOException(PARSER, input);
    }

    public static KvStream parseDelimitedFrom(
            java.io.InputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }

    public static KvStream parseFrom(
            com.google.protobuf.CodedInputStream input)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseWithIOException(PARSER, input);
    }

    public static KvStream parseFrom(
            com.google.protobuf.CodedInputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
                .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static Builder newBuilder() {
        return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(KvStream prototype) {
        return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    public static KvStream getDefaultInstance() {
        return DEFAULT_INSTANCE;
    }

    public static com.google.protobuf.Parser<KvStream> parser() {
        return PARSER;
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
            UnusedPrivateParameter unused) {
        return new KvStream();
    }

    @java.lang.Override
    public com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
        return this.unknownFields;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
    internalGetFieldAccessorTable() {
        return HgStoreStreamProto.internal_static_KvStream_fieldAccessorTable
                .ensureFieldAccessorsInitialized(
                        KvStream.class, KvStream.Builder.class);
    }

    /**
     * <pre>
     * query times.
     * </pre>
     *
     * <code>int32 seq_no = 1;</code>
     *
     * @return The seqNo.
     */
    @java.lang.Override
    public int getSeqNo() {
        return seqNo_;
    }

    /**
     * <pre>
     * true=no more data
     * </pre>
     *
     * <code>bool over = 2;</code>
     *
     * @return The over.
     */
    @java.lang.Override
    public boolean getOver() {
        return over_;
    }

    /**
     * <code>uint32 version = 4;</code>
     *
     * @return The version.
     */
    @java.lang.Override
    public int getVersion() {
        return version_;
    }

    /**
     * <code>bytes stream = 5;</code>
     *
     * @return The stream.
     */
    @java.lang.Override
    public ByteBuffer getStream() {
        return stream_;
    }

    /**
     * <code>.KvStreamType type = 6;</code>
     *
     * @return The enum numeric value on the wire for type.
     */
    @java.lang.Override
    public int getTypeValue() {
        return type_;
    }

    /**
     * <code>.KvStreamType type = 6;</code>
     *
     * @return The type.
     */
    @java.lang.Override
    public org.apache.hugegraph.store.grpc.stream.KvStreamType getType() {
        @SuppressWarnings("deprecation")
        org.apache.hugegraph.store.grpc.stream.KvStreamType result =
                org.apache.hugegraph.store.grpc.stream.KvStreamType.valueOf(type_);
        return result == null ? org.apache.hugegraph.store.grpc.stream.KvStreamType.UNRECOGNIZED :
               result;
    }

    @java.lang.Override
    public boolean isInitialized() {
        byte isInitialized = memoizedIsInitialized;
        if (isInitialized == 1) return true;
        if (isInitialized == 0) return false;

        memoizedIsInitialized = 1;
        return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
            throws java.io.IOException {
        if (seqNo_ != 0) {
            output.writeInt32(1, seqNo_);
        }
        if (over_) {
            output.writeBool(2, over_);
        }
        if (version_ != 0) {
            output.writeUInt32(4, version_);
        }
        if (stream_.limit() > 0) {
            output.writeByteArray(5, stream_.array(), 0, stream_.limit());
        }
        if (type_ !=
            org.apache.hugegraph.store.grpc.stream.KvStreamType.STREAM_TYPE_NONE.getNumber()) {
            output.writeEnum(6, type_);
        }
        unknownFields.writeTo(output);
        if (complete_ != null) {
            complete_.accept(this);
        }
    }

    @java.lang.Override
    public int getSerializedSize() {
        int size = memoizedSize;
        if (size != -1) return size;

        size = 0;
        if (seqNo_ != 0) {
            size += com.google.protobuf.CodedOutputStream
                    .computeInt32Size(1, seqNo_);
        }
        if (over_) {
            size += com.google.protobuf.CodedOutputStream
                    .computeBoolSize(2, over_);
        }
        if (version_ != 0) {
            size += com.google.protobuf.CodedOutputStream
                    .computeUInt32Size(4, version_);
        }
        if (stream_.limit() > 0) {
            size += com.google.protobuf.CodedOutputStream
                            .computeTagSize(5) +
                    com.google.protobuf.CodedOutputStream
                            .computeUInt32SizeNoTag(stream_.limit())
                    + stream_.limit();
        }
        if (type_ !=
            org.apache.hugegraph.store.grpc.stream.KvStreamType.STREAM_TYPE_NONE.getNumber()) {
            size += com.google.protobuf.CodedOutputStream
                    .computeEnumSize(6, type_);
        }
        size += unknownFields.getSerializedSize();
        memoizedSize = size;
        return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof KvStream)) {
            return super.equals(obj);
        }
        KvStream other = (KvStream) obj;

        if (getSeqNo()
            != other.getSeqNo()) {
            return false;
        }
        if (getOver()
            != other.getOver()) {
            return false;
        }
        if (getVersion()
            != other.getVersion()) {
            return false;
        }
        if (!getStream()
                .equals(other.getStream())) {
            return false;
        }
        return unknownFields.equals(other.unknownFields);
    }

    @java.lang.Override
    public int hashCode() {
        if (memoizedHashCode != 0) {
            return memoizedHashCode;
        }
        int hash = 41;
        hash = (19 * hash) + getDescriptor().hashCode();
        hash = (37 * hash) + SEQ_NO_FIELD_NUMBER;
        hash = (53 * hash) + getSeqNo();
        hash = (37 * hash) + OVER_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
                getOver());
        hash = (37 * hash) + VERSION_FIELD_NUMBER;
        hash = (53 * hash) + getVersion();
        hash = (37 * hash) + STREAM_FIELD_NUMBER;
        hash = (53 * hash) + getStream().hashCode();
        hash = (29 * hash) + unknownFields.hashCode();
        memoizedHashCode = hash;
        return hash;
    }

    @java.lang.Override
    public Builder newBuilderForType() {
        return newBuilder();
    }

    @java.lang.Override
    public Builder toBuilder() {
        return this == DEFAULT_INSTANCE
               ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
            com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        Builder builder = new Builder(parent);
        return builder;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<KvStream> getParserForType() {
        return PARSER;
    }

    @java.lang.Override
    public KvStream getDefaultInstanceForType() {
        return DEFAULT_INSTANCE;
    }

    /**
     * Protobuf type {@code KvStream}
     */
    public static final class Builder extends
                                      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
                                                                                              // @@protoc_insertion_point(builder_implements:KvStream)
                                                                                                      KvStreamOrBuilder {
        private int seqNo_;
        private boolean over_;
        private int version_;
        private ByteBuffer stream_ = ByteBuffer.allocate(0);
        private int type_ = 0;
        private Consumer<KvStream> complete_;

        // Construct using org.apache.hugegraph.store.grpc.stream.KvStream.newBuilder()
        private Builder() {
            maybeForceBuilderInitialization();
        }

        private Builder(
                com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
            super(parent);
            maybeForceBuilderInitialization();
        }

        public static com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
            return HgStoreStreamProto.internal_static_KvStream_descriptor;
        }

        @java.lang.Override
        protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
            return HgStoreStreamProto.internal_static_KvStream_fieldAccessorTable
                    .ensureFieldAccessorsInitialized(
                            KvStream.class, KvStream.Builder.class);
        }

        private void maybeForceBuilderInitialization() {
            if (com.google.protobuf.GeneratedMessageV3
                    .alwaysUseFieldBuilders) {
            }
        }

        @java.lang.Override
        public Builder clear() {
            super.clear();
            seqNo_ = 0;

            over_ = false;

            version_ = 0;

            stream_ = ByteBuffer.allocate(0);

            complete_ = null;

            return this;
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
            return HgStoreStreamProto.internal_static_KvStream_descriptor;
        }

        @java.lang.Override
        public KvStream getDefaultInstanceForType() {
            return KvStream.getDefaultInstance();
        }

        @java.lang.Override
        public KvStream build() {
            KvStream result = buildPartial();
            if (!result.isInitialized()) {
                throw newUninitializedMessageException(result);
            }
            return result;
        }

        @java.lang.Override
        public KvStream buildPartial() {
            KvStream result = new KvStream(this);
            result.seqNo_ = seqNo_;
            result.over_ = over_;
            result.version_ = version_;
            result.stream_ = stream_;
            result.complete_ = complete_;
            onBuilt();
            // d���
            return result;
        }

        @java.lang.Override
        public Builder clone() {
            return super.clone();
        }

        @java.lang.Override
        public Builder setField(
                com.google.protobuf.Descriptors.FieldDescriptor field,
                java.lang.Object value) {
            return super.setField(field, value);
        }

        @java.lang.Override
        public Builder clearField(
                com.google.protobuf.Descriptors.FieldDescriptor field) {
            return super.clearField(field);
        }

        @java.lang.Override
        public Builder clearOneof(
                com.google.protobuf.Descriptors.OneofDescriptor oneof) {
            return super.clearOneof(oneof);
        }

        @java.lang.Override
        public Builder setRepeatedField(
                com.google.protobuf.Descriptors.FieldDescriptor field,
                int index, java.lang.Object value) {
            return super.setRepeatedField(field, index, value);
        }

        @java.lang.Override
        public Builder addRepeatedField(
                com.google.protobuf.Descriptors.FieldDescriptor field,
                java.lang.Object value) {
            return super.addRepeatedField(field, value);
        }

        @java.lang.Override
        public Builder mergeFrom(com.google.protobuf.Message other) {
            if (other instanceof KvStream) {
                return mergeFrom((KvStream) other);
            } else {
                super.mergeFrom(other);
                return this;
            }
        }

        public Builder mergeFrom(KvStream other) {
            if (other == KvStream.getDefaultInstance()) return this;
            if (other.getSeqNo() != 0) {
                setSeqNo(other.getSeqNo());
            }
            if (other.getOver()) {
                setOver(other.getOver());
            }
            if (other.getVersion() != 0) {
                setVersion(other.getVersion());
            }
            if (other.getStream() != ByteBuffer.allocate(0)) {
                setStream(other.getStream());
            }
            this.mergeUnknownFields(other.unknownFields);
            onChanged();
            return this;
        }

        @java.lang.Override
        public boolean isInitialized() {
            return true;
        }

        @java.lang.Override
        public Builder mergeFrom(
                com.google.protobuf.CodedInputStream input,
                com.google.protobuf.ExtensionRegistryLite extensionRegistry)
                throws java.io.IOException {
            KvStream parsedMessage = null;
            try {
                parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                parsedMessage = (KvStream) e.getUnfinishedMessage();
                throw e.unwrapIOException();
            } finally {
                if (parsedMessage != null) {
                    mergeFrom(parsedMessage);
                }
            }
            return this;
        }

        /**
         * <pre>
         * query times.
         * </pre>
         *
         * <code>int32 seq_no = 1;</code>
         *
         * @return The seqNo.
         */
        @java.lang.Override
        public int getSeqNo() {
            return seqNo_;
        }

        /**
         * <pre>
         * query times.
         * </pre>
         *
         * <code>int32 seq_no = 1;</code>
         *
         * @param value The seqNo to set.
         * @return This builder for chaining.
         */
        public Builder setSeqNo(int value) {

            seqNo_ = value;
            onChanged();
            return this;
        }

        /**
         * <pre>
         * query times.
         * </pre>
         *
         * <code>int32 seq_no = 1;</code>
         *
         * @return This builder for chaining.
         */
        public Builder clearSeqNo() {

            seqNo_ = 0;
            onChanged();
            return this;
        }

        /**
         * <pre>
         * true=no more data
         * </pre>
         *
         * <code>bool over = 2;</code>
         *
         * @return The over.
         */
        @java.lang.Override
        public boolean getOver() {
            return over_;
        }

        /**
         * <pre>
         * true=no more data
         * </pre>
         *
         * <code>bool over = 2;</code>
         *
         * @param value The over to set.
         * @return This builder for chaining.
         */
        public Builder setOver(boolean value) {

            over_ = value;
            onChanged();
            return this;
        }

        /**
         * <pre>
         * true=no more data
         * </pre>
         *
         * <code>bool over = 2;</code>
         *
         * @return This builder for chaining.
         */
        public Builder clearOver() {

            over_ = false;
            onChanged();
            return this;
        }

        /**
         * <code>uint32 version = 4;</code>
         *
         * @return The version.
         */
        @java.lang.Override
        public int getVersion() {
            return version_;
        }

        /**
         * <code>uint32 version = 4;</code>
         *
         * @param value The version to set.
         * @return This builder for chaining.
         */
        public Builder setVersion(int value) {

            version_ = value;
            onChanged();
            return this;
        }

        /**
         * <code>uint32 version = 4;</code>
         *
         * @return This builder for chaining.
         */
        public Builder clearVersion() {

            version_ = 0;
            onChanged();
            return this;
        }

        /**
         * <code>bytes stream = 5;</code>
         *
         * @return The stream.
         */
        @java.lang.Override
        public ByteBuffer getStream() {
            return stream_;
        }

        /**
         * <code>bytes stream = 5;</code>
         *
         * @param value The stream to set.
         * @return This builder for chaining.
         */
        public Builder setStream(ByteBuffer value) {
            if (value == null) {
                throw new NullPointerException();
            }

            stream_ = value;
            onChanged();
            return this;
        }

        /**
         * <code>bytes stream = 5;</code>
         *
         * @return This builder for chaining.
         */
        public Builder clearStream() {

            stream_ = getDefaultInstance().getStream();
            onChanged();
            return this;
        }

        /**
         * <code>.KvStreamType type = 6;</code>
         *
         * @return The enum numeric value on the wire for type.
         */
        @java.lang.Override
        public int getTypeValue() {
            return type_;
        }

        /**
         * <code>.KvStreamType type = 6;</code>
         *
         * @param value The enum numeric value on the wire for type to set.
         * @return This builder for chaining.
         */
        public Builder setTypeValue(int value) {

            type_ = value;
            onChanged();
            return this;
        }

        /**
         * <code>.KvStreamType type = 6;</code>
         *
         * @return The type.
         */
        @java.lang.Override
        public org.apache.hugegraph.store.grpc.stream.KvStreamType getType() {
            @SuppressWarnings("deprecation")
            org.apache.hugegraph.store.grpc.stream.KvStreamType result =
                    org.apache.hugegraph.store.grpc.stream.KvStreamType.valueOf(type_);
            return result == null ?
                   org.apache.hugegraph.store.grpc.stream.KvStreamType.UNRECOGNIZED : result;
        }

        /**
         * <code>.KvStreamType type = 6;</code>
         *
         * @param value The type to set.
         * @return This builder for chaining.
         */
        public Builder setType(org.apache.hugegraph.store.grpc.stream.KvStreamType value) {
            if (value == null) {
                throw new NullPointerException();
            }

            type_ = value.getNumber();
            onChanged();
            return this;
        }

        /**
         * <code>.KvStreamType type = 6;</code>
         *
         * @return This builder for chaining.
         */
        public Builder clearType() {

            type_ = 0;
            onChanged();
            return this;
        }

        @java.lang.Override
        public Builder setUnknownFields(
                final com.google.protobuf.UnknownFieldSet unknownFields) {
            return super.setUnknownFields(unknownFields);
        }

        @java.lang.Override
        public Builder mergeUnknownFields(
                final com.google.protobuf.UnknownFieldSet unknownFields) {
            return super.mergeUnknownFields(unknownFields);
        }

        public void complete(Consumer<KvStream> consumer) {
            this.complete_ = consumer;
        }

        // @@protoc_insertion_point(builder_scope:KvStream)
    }

}
