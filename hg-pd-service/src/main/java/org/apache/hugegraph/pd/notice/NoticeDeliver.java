package org.apache.hugegraph.pd.notice;

/**
 * @author lynn.bond@hotmail.com on 2023/11/29
 */
public interface NoticeDeliver {
    boolean isDuty();

    Long send(String durableId);

    String save();

    boolean remove(String durableId);

    String toNoticeString();
}
