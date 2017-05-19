package com.baidu.hugegraph.type.define;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonGetter;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonValue;

/**
 * Created by liningrui on 2017/5/19.
 */
public class EdgeLink {

    private static final long serialVersionUID = 4954918890077093841L;

    // use public for json serialize
    public String source;
    public String target;

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

    @JsonGetter
    public String target() {
        return target;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof EdgeLink)) {
            return false;
        }

        EdgeLink other = (EdgeLink) obj;
        if (this.source().equals(other.source())
                && this.target().equals(other.target())) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return this.source().hashCode() ^ this.target().hashCode();
    }

}
