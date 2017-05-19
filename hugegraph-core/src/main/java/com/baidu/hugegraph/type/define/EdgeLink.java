package com.baidu.hugegraph.type.define;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by liningrui on 2017/5/19.
 */
public class EdgeLink extends Pair<String, String> {

    private static final long serialVersionUID = 4954918890077093841L;

    private String source;
    private String target;

    public static EdgeLink of(String source, String target) {
        return new EdgeLink(source, target);
    }

    public EdgeLink() {
        super();
    }

    public EdgeLink(String source, String target) {
        super();
        this.source = source;
        this.target = target;
    }

    public String source() {
        return source;
    }

    public String target() {
        return target;
    }

    @Override
    public String getLeft() {
        return source;
    }

    @Override
    public String getRight() {
        return target;
    }

    @Override
    public String setValue(String value) {
        throw new UnsupportedOperationException();
    }
}
