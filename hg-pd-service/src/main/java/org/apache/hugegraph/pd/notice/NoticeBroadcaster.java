<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
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

package org.apache.hugegraph.pd.notice;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.hugegraph.pd.common.HgAssert;

import lombok.extern.slf4j.Slf4j;

// TODO: merge/move to another package
@Slf4j
public class NoticeBroadcaster {

    private final Supplier<Long> noticeSupplier;
    private long noticeId;
    private String durableId;
    private Supplier<String> durableSupplier;
    private Function<String, Boolean> removeFunction;
    private int state; //0=ready; 1=notified; 2=done ack; -1=error
    private int counter;
    private long timestamp;

    private NoticeBroadcaster(Supplier<Long> noticeSupplier) {
        this.noticeSupplier = noticeSupplier;
        this.timestamp = System.currentTimeMillis();
    }

    public static NoticeBroadcaster of(Supplier<Long> noticeSupplier) {
        HgAssert.isArgumentNotNull(noticeSupplier, "noticeSupplier");
        return new NoticeBroadcaster(noticeSupplier);
    }

    public NoticeBroadcaster setDurableSupplier(Supplier<String> durableSupplier) {
        this.durableSupplier = durableSupplier;
        return this;
    }

    public NoticeBroadcaster setRemoveFunction(Function<String, Boolean> removeFunction) {
        this.removeFunction = removeFunction;
        return this;
    }

    public NoticeBroadcaster notifying() {

        if (this.state >= 2) {
            log.warn("Aborted notifying as ack has done. notice: {}", this);
========
package org.apache.hugegraph.pd.notice;

import org.apache.hugegraph.pd.common.HgAssert;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com on 2022/2/10
 * @version 2.0 added the NoticeDeliver on 2023/11/29
 */
@Slf4j
public class NoticeBroadcaster {
    private final NoticeDeliver noticeDeliver;
    private long noticeId;
    private String durableId;
    private int state; // 0=ready; 1=notified; 2=done ack; 10=invalid, -1=error
    private int counter;
    private long timestamp;

    public static NoticeBroadcaster of(NoticeDeliver noticeDeliver) {
        HgAssert.isArgumentNotNull(noticeDeliver, "noticeDeliver");
        return new NoticeBroadcaster(noticeDeliver);
    }

    private NoticeBroadcaster(NoticeDeliver noticeDeliver) {
        this.noticeDeliver = noticeDeliver;
        this.timestamp = System.currentTimeMillis();
    }

    public NoticeBroadcaster notifying() {
        try {
            if (!this.noticeDeliver.isDuty()) {
                this.state = 10;
                log.warn("Notification aborted due to not in duty state. notice: {}", this.getNoticeString());
                return this;
            }
        } catch (Throwable t) {
            log.error("Failed to invoke `NoticeDeliver::isDuty`, but continuing the the notification, caused by:", t);
        }

        if (this.state >= 2) {
            log.warn("Notification aborted as acknowledgment has been received. notice: {}", this);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
            return this;
        }

        this.counter++;
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java

        if (this.durableId == null && this.durableSupplier != null) {
            try {
                this.durableId = this.durableSupplier.get();
            } catch (Throwable t) {
                log.error("Failed to invoke durableSupplier, cause by:", t);
========
        if (this.durableId == null) {
            try {
                this.durableId = this.noticeDeliver.save();
            } catch (Throwable t) {
                log.error("Failed to invoke `NoticeDeliver::save`, caused by:", t);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
            }
        }

        try {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
            this.noticeId = this.noticeSupplier.get();
            state = 1;
        } catch (Throwable t) {
            state = -1;
            log.error("Failed to invoke noticeSupplier: {}; cause by: " +
                      this.noticeSupplier.toString(), t);
========
            this.noticeId = this.noticeDeliver.send(this.durableId);
            state = 1;
        } catch (Throwable t) {
            state = -1;
            log.error("Failed to invoke `NoticeDeliver::send`, notice: {}, caused by: " + this.noticeDeliver.toNoticeString(), t);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
        }

        return this;
    }

    public boolean checkAck(long ackNoticeId) {
        boolean flag = false;

        if (this.noticeId == ackNoticeId) {
            flag = true;
            this.state = 2;
        }

        if (flag) {
            this.doRemoveDurable();
        }

        return flag;
    }

    public boolean doRemoveDurable() {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
        log.info("Removing NoticeBroadcaster is stating, noticeId:{}, durableId: {}"
                , this.noticeId, this.durableId);
        boolean flag = false;

        if (this.removeFunction == null) {
            log.warn("The remove-function hasn't been set.");
            return false;
        }
========
        log.info("NoticeBroadcaster is being removed, noticeId:{}, durableId: {}"
                , this.noticeId, this.durableId);
        boolean flag = false;

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java

        if (this.durableId == null) {
            log.warn("The durableId hasn't been set.");
            return false;
        }

        try {
<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
            if (!(flag = this.removeFunction.apply(this.durableId))) {
                log.error("Removing NoticeBroadcaster was not complete, noticeId: {}, durableId: {}"
                        , this.noticeId, this.durableId);
            }
        } catch (Throwable t) {
            log.error("Failed to remove NoticeBroadcaster, noticeId: "
                      + this.noticeId + ", durableId: " + this.durableId + ". Cause by:", t);
========
            if (!(flag = this.noticeDeliver.remove(this.durableId))) {
                log.error("Removing NoticeBroadcaster was not complete, noticeId: {}, durableId: {}",
                        this.noticeId, this.durableId);
            }
        } catch (Throwable t) {
            log.error("Failed to remove NoticeBroadcaster, noticeId: {}, durableId: {}. Caused by:",
                    this.noticeId, this.durableId, t);
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
        }

        return flag;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
========
    public void setDurableId(String durableId) {
        if (HgAssert.isInvalid(durableId)) {
            log.warn("Set an invalid durable id to the NoticeBroadcaster.");
        }

        this.durableId = durableId;
    }

>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
    public long getNoticeId() {
        return noticeId;
    }

    public int getState() {
        return state;
    }

    public int getCounter() {
        return counter;
    }

    public String getDurableId() {
        return durableId;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
    public void setDurableId(String durableId) {

        if (HgAssert.isInvalid(durableId)) {
            log.warn("Set an invalid durable-id to NoticeBroadcaster.");
        }

        this.durableId = durableId;
    }

========
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

<<<<<<<< HEAD:hugegraph-pd/hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
    @Override
    public String toString() {
        return "NoticeBroadcaster{" +
               "noticeId=" + noticeId +
               ", durableId='" + durableId + '\'' +
               ", state=" + state +
               ", counter=" + counter +
               ", timestamp=" + timestamp +
               '}';
========
    public String getNoticeString() {
        return this.noticeDeliver.toNoticeString();
    }

    @Override
    public String toString() {
        return "NoticeBroadcaster{" +
                "noticeId=" + noticeId +
                ", durableId='" + durableId + '\'' +
                ", state=" + state +
                ", counter=" + counter +
                ", timestamp=" + timestamp +
                '}';
>>>>>>>> d7e3d51dd (3.6.5 -> 4.x diff):hg-pd-service/src/main/java/org/apache/hugegraph/pd/notice/NoticeBroadcaster.java
    }
}
