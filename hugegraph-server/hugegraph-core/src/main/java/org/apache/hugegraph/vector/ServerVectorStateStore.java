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

package org.apache.hugegraph.vector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.hugegraph.HugeGraphParams;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.id.IdGenerator;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.IdPrefixQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.query.QueryResults;
import org.apache.hugegraph.backend.serializer.BytesBuffer;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.SchemaElement;
import org.apache.hugegraph.structure.HugeIndex;
import org.apache.hugegraph.structure.HugeVectorIndexMap;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.DataType;
import org.apache.hugegraph.type.define.HugeKeys;
import org.apache.hugegraph.type.define.IndexVectorState;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class ServerVectorStateStore implements VectorIndexStateStore<Id> {

    private HugeGraphParams graphParams = null;

    ServerVectorStateStore(HugeGraphParams graphParams) {
        E.checkNotNull(graphParams, "graphParams");
        this.graphParams = graphParams;
    }

    @Override
    public void stop() {

    }

    @Override
    public Iterable<VectorRecord> scanDeltas(Id indexLabelId, long fromSeq) {

        //first construct the query
        // second use the store query to get result list
        // third transform the result list to VectorRecord
        BytesBuffer prefixBuffer = BytesBuffer.allocate(5);
        prefixBuffer.write(0);
        prefixBuffer.writeInt(SchemaElement.schemaId(indexLabelId));
        Id prefix = prefixBuffer.asId();
        Id start = HugeVectorIndexMap.formatSequenceId(indexLabelId, fromSeq + 1L);

        Query query = new IdPrefixQuery(HugeType.VECTOR_SEQUENCE,
                                        null, start, true, prefix);

        QueryResults<BackendEntry> entries = this.graphParams.graphTransaction().query(query);

        // we could get the vector data in one time with this Query after we isolate the vector cf.
        // Query queryVectorData = new IdPrefixQuery(HugeType.VECTOR_SEQUENCE,
        //                                          null, start, true, prefix);
        return convertToVectorRecord(entries.iterator());
    }

    @Override
    public Set<Id> getVertex(Id indexLabelId, Iterator<Integer> vectorIds) {
        Set<Id> vertexIds = new HashSet<>();
        while (vectorIds.hasNext()) {
            int vectorId = vectorIds.next();
            Id id = IdGenerator.of(vectorId);
            IndexLabel il = graphParams.graph().indexLabel(indexLabelId);
            HugeVectorIndexMap SequenceIndex = new HugeVectorIndexMap(graphParams.graph(), il);
            SequenceIndex.fieldValues(vectorId);
            Query q = new IdPrefixQuery(HugeType.VECTOR_INDEX_MAP, SequenceIndex.id());
            QueryResults<BackendEntry> results = this.graphParams.graphTransaction().query(q);
            ConditionQuery conditionQuery = new ConditionQuery(HugeType.VECTOR_INDEX_MAP);
            conditionQuery.eq(HugeKeys.INDEX_LABEL_ID, indexLabelId);
            conditionQuery.eq(HugeKeys.FIELD_VALUES, vectorId);
            HugeIndex index = ConvertVectorIndex(indexLabelId, results.iterator(), conditionQuery);
        //    TODO: transform the element id to set<ID>
            vertexIds.add(index.elementId());
        }
        return vertexIds;
    }

    HugeIndex ConvertVectorIndex(Id indexLabelId, Iterator<BackendEntry> entries,
                                 ConditionQuery query) {
        while (entries.hasNext()) {
            BackendEntry entry = entries.next();
            HugeIndex index = graphParams.serializer().readIndex(graphParams.graph(), query, entry);
            if(index.elementId() != null) {
                // if the state not equal deleting, the element id would not be set
                return index;
            }
        }
        return null;
    }


    private List<VectorRecord> convertToVectorRecord(Iterator<BackendEntry> entries) {
        List<VectorRecord> records = new ArrayList<>();
        while (entries.hasNext()) {
            BackendEntry entry = entries.next();
            Iterator<BackendEntry.BackendColumn> columns = entry.columns().iterator();
            HugeVectorIndexMap map =
                    graphParams.serializer().readVectorSequence(graphParams.graph(), null,
                                                                entry);

            //query vector index map to get the vertex id
            Query query = new IdPrefixQuery(HugeType.VECTOR_INDEX_MAP, map.id());
            QueryResults<BackendEntry> VectorToVertexEntries =
                    this.graphParams.graphTransaction().query(query);

            Vertex targetVertex = graphParams.graphTransaction().queryVertex(map.elementId());

            IndexLabel il = map.indexLabel();
            PropertyKey propertyKey = graphParams.graph().propertyKey(il.indexField());

            E.checkArgument(propertyKey.dataType() == DataType.FLOAT &&
                            propertyKey.cardinality() == Cardinality.LIST, "the property key must" +
                                                                           " be a float list");

            Object propValue = targetVertex.property(propertyKey.name()).value();
            List<Float> floatList = (List<Float>) propValue;
            float[] vectorData = new float[floatList.size()];
            IntStream.range(0, floatList.size()).forEach(i -> vectorData[i] = floatList.get(i));

            records.add(new VectorRecord((int) map.fieldValues(), vectorData,
                                         map.vectorState() == IndexVectorState.DELETING,
                                         map.sequence()));

        }

        return records;
    }
}
