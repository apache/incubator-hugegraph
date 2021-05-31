package com.baidu.hugegraph.backend.page;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.util.CollectionUtil;
import com.google.common.collect.ImmutableSet;

public class JointIdHolderList extends IdHolderList {

    private final List<IdHolder> mergedHolders;

    public JointIdHolderList(boolean paging) {
        super(paging);
        this.mergedHolders = new ArrayList<>();
    }

    @Override
    public boolean add(IdHolder holder) {
        if (this.paging()) {
            return super.add(holder);
        }
        this.mergedHolders.add(holder);

        if (super.isEmpty()) {
            Query parent = holder.query().originQuery();
            super.add(new JointIdHolder(parent));
        }

        ((JointIdHolder) this.get(0)).merge(holder);

        return true;
    }

    private class JointIdHolder extends IdHolder.FixedIdHolder {

        private Set<Id> ids;

        public JointIdHolder(Query parent) {
            super(new MergedQuery(parent), ImmutableSet.of());
        }

        @Override
        public boolean paging() {
            return false;
        }

        @Override
        public Set<Id> all() {
            return this.ids;
        }

        public void merge(IdHolder holder){
            if (this.ids == null) {
                this.ids = holder.all();
            } else {
                CollectionUtil.intersectWithModify(this.ids, holder.all());
            }
        }

        @Override
        public PageIds fetchNext(String page, long pageSize) {
            throw new NotImplementedException("JointIdHolder.fetchNext");
        }
    }

    private class MergedQuery extends Query {

        public MergedQuery(Query parent) {
            super(parent.resultType(), parent);
        }

        @Override
        public String toString() {
            return JointIdHolderList.this.mergedHolders.toString();
        }
    }

}
