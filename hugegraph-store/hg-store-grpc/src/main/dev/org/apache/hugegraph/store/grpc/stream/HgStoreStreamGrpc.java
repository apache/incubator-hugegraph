/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.store.grpc.stream;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 **
 *为了提升性能，复用内存，减少gc回收，需要重写KvStream.writeTo方法
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.39.0)",
    comments = "Source: store_stream.proto")
public final class HgStoreStreamGrpc {

  private HgStoreStreamGrpc() {}

  public static final String SERVICE_NAME = "HgStoreStream";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamReq,
      org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Scan",
      requestType = org.apache.hugegraph.store.grpc.stream.ScanStreamReq.class,
      responseType = org.apache.hugegraph.store.grpc.stream.KvPageRes.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamReq,
      org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanMethod() {
    io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamReq, org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanMethod;
    if ((getScanMethod = HgStoreStreamGrpc.getScanMethod) == null) {
      synchronized (HgStoreStreamGrpc.class) {
        if ((getScanMethod = HgStoreStreamGrpc.getScanMethod) == null) {
          HgStoreStreamGrpc.getScanMethod = getScanMethod =
              io.grpc.MethodDescriptor.<org.apache.hugegraph.store.grpc.stream.ScanStreamReq, org.apache.hugegraph.store.grpc.stream.KvPageRes>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Scan"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.hugegraph.store.grpc.stream.ScanStreamReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.hugegraph.store.grpc.stream.KvPageRes.getDefaultInstance()))
              .setSchemaDescriptor(new HgStoreStreamMethodDescriptorSupplier("Scan"))
              .build();
        }
      }
    }
    return getScanMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamReq,
      org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanOneShotMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScanOneShot",
      requestType = org.apache.hugegraph.store.grpc.stream.ScanStreamReq.class,
      responseType = org.apache.hugegraph.store.grpc.stream.KvPageRes.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamReq,
      org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanOneShotMethod() {
    io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamReq, org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanOneShotMethod;
    if ((getScanOneShotMethod = HgStoreStreamGrpc.getScanOneShotMethod) == null) {
      synchronized (HgStoreStreamGrpc.class) {
        if ((getScanOneShotMethod = HgStoreStreamGrpc.getScanOneShotMethod) == null) {
          HgStoreStreamGrpc.getScanOneShotMethod = getScanOneShotMethod =
              io.grpc.MethodDescriptor.<org.apache.hugegraph.store.grpc.stream.ScanStreamReq, org.apache.hugegraph.store.grpc.stream.KvPageRes>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScanOneShot"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.hugegraph.store.grpc.stream.ScanStreamReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.hugegraph.store.grpc.stream.KvPageRes.getDefaultInstance()))
              .setSchemaDescriptor(new HgStoreStreamMethodDescriptorSupplier("ScanOneShot"))
              .build();
        }
      }
    }
    return getScanOneShotMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq,
      org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanBatchMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScanBatch",
      requestType = org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq.class,
      responseType = org.apache.hugegraph.store.grpc.stream.KvPageRes.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq,
      org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanBatchMethod() {
    io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq, org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanBatchMethod;
    if ((getScanBatchMethod = HgStoreStreamGrpc.getScanBatchMethod) == null) {
      synchronized (HgStoreStreamGrpc.class) {
        if ((getScanBatchMethod = HgStoreStreamGrpc.getScanBatchMethod) == null) {
          HgStoreStreamGrpc.getScanBatchMethod = getScanBatchMethod =
              io.grpc.MethodDescriptor.<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq, org.apache.hugegraph.store.grpc.stream.KvPageRes>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScanBatch"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.hugegraph.store.grpc.stream.KvPageRes.getDefaultInstance()))
              .setSchemaDescriptor(new HgStoreStreamMethodDescriptorSupplier("ScanBatch"))
              .build();
        }
      }
    }
    return getScanBatchMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq,
          KvStream> getScanBatch2Method;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScanBatch2",
      requestType = org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq.class,
      responseType = KvStream.class,
      methodType = io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
  public static io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq,
          KvStream> getScanBatch2Method() {
    io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq, KvStream> getScanBatch2Method;
    if ((getScanBatch2Method = HgStoreStreamGrpc.getScanBatch2Method) == null) {
      synchronized (HgStoreStreamGrpc.class) {
        if ((getScanBatch2Method = HgStoreStreamGrpc.getScanBatch2Method) == null) {
          HgStoreStreamGrpc.getScanBatch2Method = getScanBatch2Method =
              io.grpc.MethodDescriptor.<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq, KvStream>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.BIDI_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScanBatch2"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                      KvStream.getDefaultInstance()))
              .setSchemaDescriptor(new HgStoreStreamMethodDescriptorSupplier("ScanBatch2"))
              .build();
        }
      }
    }
    return getScanBatch2Method;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq,
      org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanBatchOneShotMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ScanBatchOneShot",
      requestType = org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq.class,
      responseType = org.apache.hugegraph.store.grpc.stream.KvPageRes.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq,
      org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanBatchOneShotMethod() {
    io.grpc.MethodDescriptor<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq, org.apache.hugegraph.store.grpc.stream.KvPageRes> getScanBatchOneShotMethod;
    if ((getScanBatchOneShotMethod = HgStoreStreamGrpc.getScanBatchOneShotMethod) == null) {
      synchronized (HgStoreStreamGrpc.class) {
        if ((getScanBatchOneShotMethod = HgStoreStreamGrpc.getScanBatchOneShotMethod) == null) {
          HgStoreStreamGrpc.getScanBatchOneShotMethod = getScanBatchOneShotMethod =
              io.grpc.MethodDescriptor.<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq, org.apache.hugegraph.store.grpc.stream.KvPageRes>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ScanBatchOneShot"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.hugegraph.store.grpc.stream.KvPageRes.getDefaultInstance()))
              .setSchemaDescriptor(new HgStoreStreamMethodDescriptorSupplier("ScanBatchOneShot"))
              .build();
        }
      }
    }
    return getScanBatchOneShotMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static HgStoreStreamStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<HgStoreStreamStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<HgStoreStreamStub>() {
        @java.lang.Override
        public HgStoreStreamStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new HgStoreStreamStub(channel, callOptions);
        }
      };
    return HgStoreStreamStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static HgStoreStreamBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<HgStoreStreamBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<HgStoreStreamBlockingStub>() {
        @java.lang.Override
        public HgStoreStreamBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new HgStoreStreamBlockingStub(channel, callOptions);
        }
      };
    return HgStoreStreamBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static HgStoreStreamFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<HgStoreStreamFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<HgStoreStreamFutureStub>() {
        @java.lang.Override
        public HgStoreStreamFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new HgStoreStreamFutureStub(channel, callOptions);
        }
      };
    return HgStoreStreamFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   **
   *为了提升性能，复用内存，减少gc回收，需要重写KvStream.writeTo方法
   * </pre>
   */
  public static abstract class HgStoreStreamImplBase implements io.grpc.BindableService {

    /**
     */
    public io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.ScanStreamReq> scan(
        io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getScanMethod(), responseObserver);
    }

    /**
     */
    public void scanOneShot(org.apache.hugegraph.store.grpc.stream.ScanStreamReq request,
        io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScanOneShotMethod(), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq> scanBatch(
        io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getScanBatchMethod(), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq> scanBatch2(
        io.grpc.stub.StreamObserver<KvStream> responseObserver) {
      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getScanBatch2Method(), responseObserver);
    }

    /**
     */
    public void scanBatchOneShot(org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq request,
        io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getScanBatchOneShotMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getScanMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                org.apache.hugegraph.store.grpc.stream.ScanStreamReq,
                org.apache.hugegraph.store.grpc.stream.KvPageRes>(
                  this, METHODID_SCAN)))
          .addMethod(
            getScanOneShotMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.hugegraph.store.grpc.stream.ScanStreamReq,
                org.apache.hugegraph.store.grpc.stream.KvPageRes>(
                  this, METHODID_SCAN_ONE_SHOT)))
          .addMethod(
            getScanBatchMethod(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq,
                org.apache.hugegraph.store.grpc.stream.KvPageRes>(
                  this, METHODID_SCAN_BATCH)))
          .addMethod(
            getScanBatch2Method(),
            io.grpc.stub.ServerCalls.asyncBidiStreamingCall(
              new MethodHandlers<
                org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq,
                      KvStream>(
                  this, METHODID_SCAN_BATCH2)))
          .addMethod(
            getScanBatchOneShotMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq,
                org.apache.hugegraph.store.grpc.stream.KvPageRes>(
                  this, METHODID_SCAN_BATCH_ONE_SHOT)))
          .build();
    }
  }

  /**
   * <pre>
   **
   *为了提升性能，复用内存，减少gc回收，需要重写KvStream.writeTo方法
   * </pre>
   */
  public static final class HgStoreStreamStub extends io.grpc.stub.AbstractAsyncStub<HgStoreStreamStub> {
    private HgStoreStreamStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HgStoreStreamStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new HgStoreStreamStub(channel, callOptions);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.ScanStreamReq> scan(
        io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getScanMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void scanOneShot(org.apache.hugegraph.store.grpc.stream.ScanStreamReq request,
        io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScanOneShotMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq> scanBatch(
        io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getScanBatchMethod(), getCallOptions()), responseObserver);
    }

    /**
     */
    public io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq> scanBatch2(
        io.grpc.stub.StreamObserver<KvStream> responseObserver) {
      return io.grpc.stub.ClientCalls.asyncBidiStreamingCall(
          getChannel().newCall(getScanBatch2Method(), getCallOptions()), responseObserver);
    }

    /**
     */
    public void scanBatchOneShot(org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq request,
        io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getScanBatchOneShotMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   **
   *为了提升性能，复用内存，减少gc回收，需要重写KvStream.writeTo方法
   * </pre>
   */
  public static final class HgStoreStreamBlockingStub extends io.grpc.stub.AbstractBlockingStub<HgStoreStreamBlockingStub> {
    private HgStoreStreamBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HgStoreStreamBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new HgStoreStreamBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.apache.hugegraph.store.grpc.stream.KvPageRes scanOneShot(org.apache.hugegraph.store.grpc.stream.ScanStreamReq request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScanOneShotMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.hugegraph.store.grpc.stream.KvPageRes scanBatchOneShot(org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getScanBatchOneShotMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   **
   *为了提升性能，复用内存，减少gc回收，需要重写KvStream.writeTo方法
   * </pre>
   */
  public static final class HgStoreStreamFutureStub extends io.grpc.stub.AbstractFutureStub<HgStoreStreamFutureStub> {
    private HgStoreStreamFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected HgStoreStreamFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new HgStoreStreamFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.hugegraph.store.grpc.stream.KvPageRes> scanOneShot(
        org.apache.hugegraph.store.grpc.stream.ScanStreamReq request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScanOneShotMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.hugegraph.store.grpc.stream.KvPageRes> scanBatchOneShot(
        org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getScanBatchOneShotMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SCAN_ONE_SHOT = 0;
  private static final int METHODID_SCAN_BATCH_ONE_SHOT = 1;
  private static final int METHODID_SCAN = 2;
  private static final int METHODID_SCAN_BATCH = 3;
  private static final int METHODID_SCAN_BATCH2 = 4;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final HgStoreStreamImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(HgStoreStreamImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SCAN_ONE_SHOT:
          serviceImpl.scanOneShot((org.apache.hugegraph.store.grpc.stream.ScanStreamReq) request,
              (io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes>) responseObserver);
          break;
        case METHODID_SCAN_BATCH_ONE_SHOT:
          serviceImpl.scanBatchOneShot((org.apache.hugegraph.store.grpc.stream.ScanStreamBatchReq) request,
              (io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SCAN:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.scan(
              (io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes>) responseObserver);
        case METHODID_SCAN_BATCH:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.scanBatch(
              (io.grpc.stub.StreamObserver<org.apache.hugegraph.store.grpc.stream.KvPageRes>) responseObserver);
        case METHODID_SCAN_BATCH2:
          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.scanBatch2(
              (io.grpc.stub.StreamObserver<KvStream>) responseObserver);
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class HgStoreStreamBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    HgStoreStreamBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return HgStoreStreamProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("HgStoreStream");
    }
  }

  private static final class HgStoreStreamFileDescriptorSupplier
      extends HgStoreStreamBaseDescriptorSupplier {
    HgStoreStreamFileDescriptorSupplier() {}
  }

  private static final class HgStoreStreamMethodDescriptorSupplier
      extends HgStoreStreamBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    HgStoreStreamMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (HgStoreStreamGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new HgStoreStreamFileDescriptorSupplier())
              .addMethod(getScanMethod())
              .addMethod(getScanOneShotMethod())
              .addMethod(getScanBatchMethod())
              .addMethod(getScanBatch2Method())
              .addMethod(getScanBatchOneShotMethod())
              .build();
        }
      }
    }
    return result;
  }
}
