package com.baidu.hugegraph.perf;

public class Stopwatch implements Cloneable {
    private long lastStartTime = -1;

    private long totalCost = 0;
    private long minCost = 0;
    private long maxCost = 0;

    private long times = 0;

    private String name;
    private String parent;

    public Stopwatch(String name, String parent) {
        this.name = name;
        this.parent = parent;
    }

    public String id() {
        return id(this.parent, this.name);
    }

    public static String id(String parent, String name) {
        if (parent == null || parent.isEmpty()) {
            return name;
        }
        return parent + "/" + name;
    }

    public String name() {
        return this.name;
    }

    public String parent() {
        return this.parent;
    }

    public void startTime(long time) {
        assert this.lastStartTime == -1;
        this.lastStartTime = time;
        this.times++;
    }

    public void endTime(long time) {
        assert time >= this.lastStartTime && this.lastStartTime != -1;
        long cost = time - this.lastStartTime;
        this.totalCost += cost;
        this.lastStartTime = -1;
        this.updateMinMax(cost);
    }

    protected void updateMinMax(long cost) {
        if (this.minCost > cost || this.minCost == 0) {
            this.minCost = cost;
        }
        if (this.maxCost < cost) {
            this.maxCost = cost;
        }
    }

    protected void totalCost(long totalCost) {
        this.totalCost = totalCost;
    }

    public long totalCost() {
        return this.totalCost;
    }

    public long minCost() {
        return this.minCost;
    }

    public long maxCost() {
        return this.maxCost;
    }

    public long times() {
        return this.times;
    }

    public Stopwatch copy() {
        try {
            return (Stopwatch) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "{totalCost:%sms, minCost:%sns, maxCost:%sns, times:%s}",
                this.totalCost / 1000000.0,
                this.minCost, this.maxCost,
                this.times);
    }

    public String toJson() {
        return String.format("{\"totalCost\":%s, "
                + "\"minCost\":%s, \"maxCost\":%s, \"times\":%s, "
                + "\"name\":\"%s\", \"parent\":\"%s\"}",
                this.totalCost,
                this.minCost,
                this.maxCost,
                this.times,
                this.name,
                this.parent);
    }
}
