package com.baidu.hugegraph.store.term;

import java.util.Objects;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/19
 */
public class HgTriple <X, Y, Z>{
    private int hash=-1;
    private X x;
    private Y y;
    private Z z;

    public HgTriple(X x, Y y, Z z) {
        this.x = x;
        this.y = y;
        this.z = z;
    }

    public X getX() {
        return x;
    }

    public Y getY() {
        return y;
    }

    public Z getZ() {
        return z;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HgTriple<?, ?, ?> hgTriple = (HgTriple<?, ?, ?>) o;
        return Objects.equals(x, hgTriple.x) && Objects.equals(y, hgTriple.y) && Objects.equals(z, hgTriple.z);
    }

    @Override
    public int hashCode() {
        if(hash==-1){
            hash= Objects.hash(x, y, z);
        }
        return this.hash;
    }

    @Override
    public String toString() {
        return "HgTriple{" +
                "x=" + x +
                ", y=" + y +
                ", z=" + z +
                '}';
    }
}
