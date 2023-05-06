package org.apache.hugegraph.pd.pulse;

import java.util.function.Consumer;

import java.util.function.Consumer;

import com.baidu.hugegraph.pd.grpc.pulse.PartitionHeartbeatResponse;
/**
 * @author lynn.bond@hotmail.com created on 2022/2/13
 */
public class PartitionNotice implements PulseServerNotice<PartitionHeartbeatResponse>{
    private long noticeId;
    private Consumer<Long> ackConsumer;
    private PartitionHeartbeatResponse content;

    public PartitionNotice(long noticeId, Consumer<Long> ackConsumer, PartitionHeartbeatResponse content) {
        this.noticeId = noticeId;
        this.ackConsumer = ackConsumer;
        this.content = content;
    }

    @Override
    public void ack() {
        this.ackConsumer.accept(this.noticeId);
    }

    @Override
    public long getNoticeId() {
        return this.noticeId;
    }

    @Override
    public PartitionHeartbeatResponse getContent() {
        return this.content;
    }
}
