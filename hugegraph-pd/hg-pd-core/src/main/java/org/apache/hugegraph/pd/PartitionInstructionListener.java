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

package org.apache.hugegraph.pd;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.pd.grpc.pulse.ChangeShard;
import org.apache.hugegraph.pd.grpc.pulse.CleanPartition;
import org.apache.hugegraph.pd.grpc.pulse.DbCompaction;
import org.apache.hugegraph.pd.grpc.pulse.MovePartition;
import org.apache.hugegraph.pd.grpc.pulse.PartitionKeyRange;
import org.apache.hugegraph.pd.grpc.pulse.SplitPartition;
import org.apache.hugegraph.pd.grpc.pulse.TransferLeader;

/**
 * Partition command listening
 */
public interface PartitionInstructionListener {

    void changeShard(Metapb.Partition partition, ChangeShard changeShard) throws PDException;

    void transferLeader(Metapb.Partition partition, TransferLeader transferLeader) throws
                                                                                   PDException;

    void splitPartition(Metapb.Partition partition, SplitPartition splitPartition) throws
                                                                                   PDException;

    void dbCompaction(Metapb.Partition partition, DbCompaction dbCompaction) throws PDException;

    void movePartition(Metapb.Partition partition, MovePartition movePartition) throws PDException;

    void cleanPartition(Metapb.Partition partition, CleanPartition cleanPartition) throws
                                                                                   PDException;

    void changePartitionKeyRange(Metapb.Partition partition,
                                 PartitionKeyRange partitionKeyRange) throws PDException;

}
