package com.baidu.hugegraph.type;

public class Split {

    private String start; // token range start
    private String end; // token range end
    private long length; // partitions count in this range

    public Split(String start, String end, long length) {
        this.start = start;
        this.end = end;
        this.length = length;
    }

    public String start() {
        return this.start;
    }

    public void start(String start) {
        this.start = start;
    }

    public String end() {
        return this.end;
    }

    public void end(String end) {
        this.end = end;
    }

    public long length() {
        return this.length;
    }

    public void length(long length) {
        this.length = length;
    }

    @Override
    public String toString() {
        return String.format("Split{start=%s, end=%s, length=%s}",
                this.start, this.end, this.length);
    }
}