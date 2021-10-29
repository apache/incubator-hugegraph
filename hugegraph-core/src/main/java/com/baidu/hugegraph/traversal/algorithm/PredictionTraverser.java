/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.traversal.algorithm;

import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class PredictionTraverser extends OltpTraverser {

    public PredictionTraverser(HugeGraph graph) {
        super(graph);
    }

    public double adamicAdar(Id source, Id target, Directions dir,
                             String label, long degree, long limit) {
        Set<Id> neighbors = checkAndGetCommonNeighbors(source, target, dir,
                                                       label, degree, limit);
        EdgeStep step = new EdgeStep(graph(), dir, ImmutableList.of(label));

        return neighbors.stream()
                        .mapToDouble(vid -> 1.0 / Math.log(
                                                  this.vertexDegree(vid, step)))
                        .sum();
    }

    public double resourceAllocation(Id source, Id target, Directions dir,
                                     String label, long degree, long limit) {
        Set<Id> neighbors = checkAndGetCommonNeighbors(source, target, dir,
                                                       label, degree, limit);
        EdgeStep step = new EdgeStep(graph(), dir, ImmutableList.of(label));

        return neighbors.stream()
                        .mapToDouble(vid -> 1.0 / this.vertexDegree(vid, step))
                        .sum();
    }

    private Set<Id> checkAndGetCommonNeighbors(Id source, Id target,
                                               Directions dir, String label,
                                               long degree, long limit) {
        E.checkNotNull(source, "source id");
        E.checkNotNull(target, "the target id");
        this.checkVertexExist(source, "source");
        this.checkVertexExist(target, "target");
        E.checkNotNull(dir, "direction");
        checkDegree(degree);
        SameNeighborTraverser traverser = new SameNeighborTraverser(graph());
        return traverser.sameNeighbors(source, target, dir,
                                       label, degree, limit);
    }

    public double computeInterUnion(Set<Id> set1, Set<Id> set2) {
        int interNum = CollectionUtil.intersect(set1, set2).size();
        int unionNum = CollectionUtil.union(set1, set2).size();
        if (unionNum == 0) {
            return 0.0D;
        }
        return (double) interNum / unionNum;
    }

    private static void reachCapacity(long count, long capacity) {
        if (capacity != NO_LIMIT && count > capacity) {
            throw new HugeException("Reach capacity '%s'", capacity);
        }
    }
}
