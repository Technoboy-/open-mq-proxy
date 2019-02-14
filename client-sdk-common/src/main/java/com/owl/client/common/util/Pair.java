package com.owl.client.common.util;

/**
 * @Author: jiwei.guo
 */
public class Pair<L, R> {

    private final L l;

    private final R r;

    public Pair(L l, R r){
        this.l = l;
        this.r = r;
    }

    public L getL() {
        return l;
    }

    public R getR() {
        return r;
    }
}
