package org.apache.hugegraph.pd.pulse;

import java.util.function.Consumer;

import org.apache.hugegraph.pd.grpc.pulse.PulseResponse;

/**
 * @author lynn.bond@hotmail.com created on 2022/2/13
 */
public class PartitionNotice extends PulseResponseNotice {

    public PartitionNotice(long noticeId, Consumer<Long> ackConsumer, PulseResponse content) {
       super(noticeId,ackConsumer, content);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("PartitionNotice{");
        sb.append("noticeId=").append(noticeId);
        sb.append(", content=").append(content);
        sb.append('}');
        return sb.toString();
    }
}
