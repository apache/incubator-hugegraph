package org.apache.hugegraph.pd.rest;

import com.baidu.hugegraph.pd.grpc.Metapb;
import com.baidu.hugegraph.pd.grpc.Pdpb;

import org.apache.hugegraph.pd.model.PeerRestRequest;
import org.apache.hugegraph.pd.model.RestApiResponse;
import org.apache.hugegraph.pd.service.PDService;

import com.baidu.hugegraph.pd.raft.RaftEngine;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@RestController
@Slf4j
@RequestMapping("/v1")
public class MemberAPI extends API {
    //TODO
    @Autowired
    PDService pdService;

    @GetMapping(value = "/members", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public RestApiResponse getMembers() throws InterruptedException, ExecutionException {

        String leaderGrpcAddress = RaftEngine.getInstance().getLeaderGrpcAddress();
        CallStreamObserverWrap<Pdpb.GetMembersResponse> response = new CallStreamObserverWrap<>();
        pdService.getMembers(Pdpb.GetMembersRequest.newBuilder().build(), response);
        List<Member> members = new ArrayList<>();
        Member leader = null;
        Map<String, Integer> stateCountMap = new HashMap<>();
        for (Metapb.Member member : response.get().get(0).getMembersList()) {
            String stateKey = member.getState().name();
            stateCountMap.put(stateKey, stateCountMap.getOrDefault(stateKey, 0) + 1);
            Member member1 = new Member(member);
            if ((leaderGrpcAddress != null) && (leaderGrpcAddress.equals(member.getGrpcUrl()))) {
                member1.role = "Leader";
                leader = member1;
            } else {
                member1.role = "Follower";
            }
            members.add(member1);
        }
        String state = pdService.getStoreNodeService().getClusterStats().getState().toString();
        HashMap<String, Object> resultMap = new HashMap<>();
        resultMap.put("state", state);
        resultMap.put("pdList", members);
        resultMap.put("pdLeader", leader);
        resultMap.put("numOfService", members.size());
        resultMap.put("numOfNormalService", stateCountMap.getOrDefault(Metapb.StoreState.Up.name(), 0));
        resultMap.put("stateCountMap", stateCountMap);
        return new RestApiResponse(resultMap, Pdpb.ErrorType.OK, Pdpb.ErrorType.OK.name());
    }

    @PostMapping(value = "/members/change", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public String changePeerList(@RequestBody PeerRestRequest body, HttpServletRequest request) {
        try {
            Pdpb.ChangePeerListRequest rpcRequest = Pdpb.ChangePeerListRequest.newBuilder().setPeerList(
                    body.getPeerList()).build();
            CountDownLatch latch = new CountDownLatch(1);
            final Pdpb.ResponseHeader[] responseHeader = {null};
            StreamObserver<Pdpb.getChangePeerListResponse> observer = new StreamObserver<Pdpb.getChangePeerListResponse>() {
                @Override
                public void onNext(Pdpb.getChangePeerListResponse value) {
                    responseHeader[0] = value.getHeader();
                }

                @Override
                public void onError(Throwable t) {
                    responseHeader[0] = Pdpb.ResponseHeader.newBuilder().setError(
                            Pdpb.Error.newBuilder().setType(
                                    Pdpb.ErrorType.UNKNOWN).setMessage(
                                    t.getMessage()).build()).build();
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    latch.countDown();
                }
            };
            pdService.changePeerList(rpcRequest, observer);
            latch.await();
            return toJSON(responseHeader[0], "changeResult");
        } catch (Exception e) {
            return toJSON(e);
        }
    }


    public static class CallStreamObserverWrap<V> extends CallStreamObserver<V> implements Future<List<V>> {
        CompletableFuture<List<V>> future = new CompletableFuture<>();
        List<V> values = new ArrayList<>();

        @Override
        public boolean isReady() {
            return false;
        }

        @Override
        public void setOnReadyHandler(Runnable runnable) {

        }

        @Override
        public void disableAutoInboundFlowControl() {

        }

        @Override
        public void request(int i) {

        }

        @Override
        public void setMessageCompression(boolean b) {

        }

        @Override
        public void onNext(V v) {
            values.add(v);
        }

        @Override
        public void onError(Throwable throwable) {
            future.completeExceptionally(throwable);
        }

        @Override
        public void onCompleted() {
            future.complete(values);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public List<V> get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        @Override
        public List<V> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return future.get(timeout, unit);
        }
    }

    @Data
    class Member {
        String raftUrl;
        String grpcUrl;
        String restUrl;
        String state;
        String dataPath;
        String role;
        String serviceName; //服务名称，自定义属性
        String serviceVersion; //静态定义
        long startTimeStamp; //启动时间，暂时取进程的启动时间

        public Member(Metapb.Member member) {
            if (member != null) {
                raftUrl = member.getRaftUrl();
                grpcUrl = member.getGrpcUrl();
                restUrl = member.getRestUrl();
                state = String.valueOf(member.getState());
                dataPath = member.getDataPath();
                serviceName = grpcUrl + "-PD";
                serviceVersion = "3.6.3";
                startTimeStamp = ManagementFactory.getRuntimeMXBean().getStartTime();

            }

        }

        public Member() {

        }
    }
}
