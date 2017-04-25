//package com.baidu.hugegraph.structure;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.Set;
//
//import com.baidu.hugegraph.backend.id.Id;
//import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
//import com.baidu.hugegraph.type.HugeTypes;
//
///**
// * Created by liningrui on 2017/4/25.
// */
//public class HugeSecondaryIndex extends HugeIndex {
//
//    private List<String> propertyValues;
//    private String indexLabelId;
//    private Set<String> elementIds;
//
//    public HugeTypes type() {
//        return HugeTypes.SECONDARY_INDEX;
//    }
//
//    protected Id generateId() {
//        return IdGeneratorFactory.generator().generate(this);
//    }
//
//    public List<String> getPropertyValues() {
//        return propertyValues;
//    }
//
//    public void setPropertyValues(List<String> propertyValues) {
//        this.propertyValues = propertyValues;
//    }
//
//    public String getIndexLabelId() {
//        return indexLabelId;
//    }
//
//    public void setIndexLabelId(String indexLabelId) {
//        this.indexLabelId = indexLabelId;
//    }
//
//    public Set<String> getElementIds() {
//        return elementIds;
//    }
//
//    public void setElementIds(Set<String> elementIds) {
//        this.elementIds = elementIds;
//    }
//
//    public void setElementIds(String... elementIds) {
//        this.elementIds.addAll(Arrays.asList(elementIds));
//    }
//}
