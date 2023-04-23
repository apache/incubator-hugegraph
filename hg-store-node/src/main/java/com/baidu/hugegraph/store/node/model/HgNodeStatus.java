package com.baidu.hugegraph.store.node.model;

import java.util.Objects;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/1
 */
public class HgNodeStatus {
    private int status;
    private String text;

    public HgNodeStatus(int status, String text) {
        this.status = status;
        this.text = text;
    }

    public int getStatus() {
        return status;
    }

    public HgNodeStatus setStatus(int status) {
        this.status = status;
        return this;
    }

    public String getText() {
        return text;
    }

    public HgNodeStatus setText(String text) {
        this.text = text;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HgNodeStatus that = (HgNodeStatus) o;
        return status == that.status && Objects.equals(text, that.text);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, text);
    }

    @Override
    public String toString() {
        return "HgNodeStatus{" +
                "status=" + status +
                ", text='" + text + '\'' +
                '}';
    }
}
