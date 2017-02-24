// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.baidu.hugegraph2;


/**
 * The multiplicity of edges between vertices for a given label. Multiplicity here is understood in the same sense as
 * for UML class diagrams {@url http://en.wikipedia.org/wiki/Class_diagram#Multiplicity}
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum Multiplicity {
    /**
     * The given edge label specifies a multi-graph, meaning that the multiplicity is not constrained and that
     * there may be multiple edges of this label between any given pair of vertices.
     *
     * @link http://en.wikipedia.org/wiki/Multigraph
     */
    MANY2MANY,

    /**
     * There can only be a single in-edge of this label for a given vertex but multiple out-edges (i.e. in-unique)
     */
    ONE2MANY,

    /**
     * There can only be a single out-edge of this label for a given vertex but multiple in-edges (i.e. out-unique)
     */
    MANY2ONE,

    /**
     * There can be only a single in and out-edge of this label for a given vertex (i.e. unique in both directions).
     */
    ONE2ONE

}
