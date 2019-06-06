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

package com.baidu.hugegraph.api.graph;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.validation.constraints.NotNull;

import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.NumericUtil;
import com.google.common.collect.Sets;

public enum UpdateStrategy {

    // Only number support sum
    SUM {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            // TODO: Improve preformance? (like write a method in common modle)
            BigDecimal oldNumber = new BigDecimal(oldProperty.toString());
            BigDecimal newNumber = new BigDecimal(newProperty.toString());
            return oldNumber.add(newNumber);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            E.checkState(oldProperty instanceof Number &&
                         newProperty instanceof Number,
                         formatError(oldProperty, newProperty, "Number"));
        }
    },

    // Only Date & Number support compare
    BIGGER {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return compareNumber(oldProperty, newProperty, BIGGER);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            E.checkState((oldProperty instanceof Date ||
                          oldProperty instanceof Number) &&
                         (newProperty instanceof Date ||
                          newProperty instanceof Number),
                         formatError(oldProperty, newProperty,
                                     "Date or Number"));
        }
    },

    SMALLER {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return compareNumber(oldProperty, newProperty, SMALLER);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            BIGGER.checkPropertyType(oldProperty, newProperty);
        }
    },

    // Only List support append, Set should use union
    APPEND {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return ((List) oldProperty).addAll((List) newProperty);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            E.checkState(oldProperty instanceof List &&
                         newProperty instanceof List,
                         formatError(oldProperty, newProperty, "List"));
        }
    },

    // Only Set support union & intersection
    UNION {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return comparedSet(oldProperty, newProperty, UNION);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            // JsonElements are always List-type, so allows two type now.
            E.checkState((oldProperty instanceof Set ||
                          oldProperty instanceof List) &&
                         (newProperty instanceof Set ||
                          newProperty instanceof List),
                         formatError(oldProperty, newProperty, "Set or List"));
        }
    },

    INTERSECTION {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return comparedSet(oldProperty, newProperty, INTERSECTION);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            UNION.checkPropertyType(oldProperty, newProperty);
        }
    },

    // TODO: Unify "action" in Vertex/EdgeAPI later
    ELIMINATE {
        @Override
        Object updatePropertyValue(Object oldProperty, Object newProperty) {
            return ((Collection) oldProperty).removeAll
                   ((Collection) newProperty);
        }

        @Override
        void checkPropertyType(Object oldProperty, Object newProperty) {
            UNION.checkPropertyType(oldProperty, newProperty);
        }
    };

    abstract Object updatePropertyValue(Object oldProperty, Object newProperty);

    abstract void checkPropertyType(Object oldProperty, Object newProperty);

    public Object checkAndUpdateProperty(@NotNull Object oldProperty,
                                         @NotNull Object newProperty) {
        checkPropertyType(oldProperty, newProperty);
        return updatePropertyValue(oldProperty, newProperty);
    }

    private static String formatError(Object oldProperty, Object newProperty,
                                      String className) {
        return String.format("Property must be" + className + "type, but got " +
                             "%s, %s", oldProperty.getClass().getSimpleName(),
                             newProperty.getClass().getSimpleName());
    }

    private static Object compareNumber(Object oldProperty, Object newProperty,
                                        UpdateStrategy strategy) {
        Number oldNum = NumericUtil.convertToNumber(oldProperty);
        Number newNum = NumericUtil.convertToNumber(newProperty);
        int result = NumericUtil.compareNumber(oldNum, newNum);
        return strategy == BIGGER ? (result > 0 ? oldProperty : newProperty) :
                                    (result < 0 ? oldProperty : newProperty);
    }

    private static Set comparedSet(Object oldProperty, Object newProperty,
                                   UpdateStrategy strategy) {
        Set oldSet = oldProperty instanceof Set ?
                     (Set) oldProperty : new HashSet((List) oldProperty);
        Set newSet = newProperty instanceof Set ?
                     (Set) newProperty : new HashSet((List) newProperty);
        return strategy == UNION ? Sets.union(oldSet, newSet) :
                                   Sets.intersection(oldSet, newSet);
    }
}
