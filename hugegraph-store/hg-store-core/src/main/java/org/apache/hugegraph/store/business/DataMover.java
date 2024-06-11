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

package org.apache.hugegraph.store.business;

import java.util.List;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.cmd.BatchPutRequest;
import org.apache.hugegraph.store.cmd.CleanDataRequest;
import org.apache.hugegraph.store.cmd.HgCmdClient;
import org.apache.hugegraph.store.cmd.UpdatePartitionResponse;

import com.alipay.sofa.jraft.Status;

/**
 * 数据转移接口，实现分区分裂和合并，支持跨机器转移数据
 */
public interface DataMover {

    void setBusinessHandler(BusinessHandler handler);

    void setCmdClient(HgCmdClient client);

    /**
     * 拷贝分区source内的数据到其他分区targets
     * 一个分区，迁移到多个分区
     *
     * @param source  source partition
     * @param targets target partitions
     * @return execution status
     * @throws Exception execution exception
     */
    Status moveData(Metapb.Partition source, List<Metapb.Partition> targets) throws Exception;

    /**
     * 将source target的数据全部拷贝到target上
     * 从一个分区迁移到另外一个分区
     *
     * @param source source partition
     * @param target target partition
     * @return execution result
     * @throws Exception execution exception
     */
    Status moveData(Metapb.Partition source, Metapb.Partition target) throws Exception;

    // 同步副本之间的分区状态
    UpdatePartitionResponse updatePartitionState(Metapb.Partition partition,
                                                 Metapb.PartitionState state);

    // 同步副本之间分区的范围
    UpdatePartitionResponse updatePartitionRange(Metapb.Partition partition, int startKey,
                                                 int endKey);

    // 清理分区partition内的无效数据
    void cleanData(Metapb.Partition partition);

    // 写入数据
    void doWriteData(BatchPutRequest request);

    void doCleanData(CleanDataRequest request);
}
