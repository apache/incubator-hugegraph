package org.apache.hugegraph.pd.pulse;

import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;

import java.util.function.Consumer;

/**
 * @author lynn.bond@hotmail.com created on 2023/12/07
 */
public class PulseResponseNotice implements PulseServerNotice<PulseResponse>{
    protected final long noticeId;
    protected final Consumer<Long> ackConsumer;
    protected final PulseResponse content;

    public PulseResponseNotice(long noticeId, Consumer<Long> ackConsumer, PulseResponse content) {
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
    public PulseResponse getContent() {
        return this.content;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("PulseNotice{");
        sb.append("noticeId=").append(noticeId);
        sb.append(", content=").append(content);
        sb.append('}');
        return sb.toString();
    }
}
