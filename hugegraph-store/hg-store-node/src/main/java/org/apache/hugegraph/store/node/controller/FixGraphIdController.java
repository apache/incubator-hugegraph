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

package org.apache.hugegraph.store.node.controller;

import static org.apache.hugegraph.rocksdb.access.SessionOperatorImpl.increaseOne;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hugegraph.backend.BackendColumn;
import org.apache.hugegraph.rocksdb.access.RocksDBSession;
import org.apache.hugegraph.rocksdb.access.ScanIterator;
import org.apache.hugegraph.rocksdb.access.SessionOperator;
import org.apache.hugegraph.serializer.BinaryElementSerializer;
import org.apache.hugegraph.store.business.BusinessHandlerImpl;
import org.apache.hugegraph.store.business.InnerKeyCreator;
import org.apache.hugegraph.store.meta.GraphIdManager;
import org.apache.hugegraph.store.meta.MetadataKeyHelper;
import org.apache.hugegraph.store.node.grpc.HgStoreNodeService;
import org.apache.hugegraph.store.node.grpc.query.QueryUtil;
import org.apache.hugegraph.structure.BaseElement;
import org.apache.hugegraph.structure.BaseProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping(value = "/fix")
public class FixGraphIdController {

    private static final String GRAPH_ID_PREFIX = "@GRAPH_ID@";
    private static final List<String> graphs = new ArrayList<>();

    static {
        String graphNames = "acgopgs/acg_opg/g\n" +
                            "acgopgs/acg/g\n" +
                            "acgopgs/example/g\n" +
                            "acgopgs/test/g\n" +
                            "acgraggs/acg_20241014/g\n" +
                            "acgraggs/acg_20241024/g\n" +
                            "acgraggs/acg_20241112/g\n" +
                            "acgraggs/example/g\n" +
                            "acgraggs/test_1/g\n" +
                            "acgraggs/test_inf_v2/g\n" +
                            "acgraggs/test_songbowei/g\n" +
                            "acgraggs/test2/g\n" +
                            "acgraggs/test3/g\n" +
                            "acgraggs/test4/g\n" +
                            "acgraggs/yangshengpu_test/g\n" +
                            "aiqichags/corpmarket_graph/g\n" +
                            "aiqichags/online_1/g\n" +
                            "aiqichags/online_2/g\n" +
                            "aiqichags/sub_graph/g\n" +
                            "appbuildergs/ab_20241121/g\n" +
                            "appbuildergs/ab_20250103/g\n" +
                            "assetgs/assetgraph_all/g\n" +
                            "assetgs/assetgraph_backup/g\n" +
                            "assetgs/assetgraph/g\n" +
                            "assetgs/test_schema/g\n" +
                            "assetgs/test/g\n" +
                            "astrolabegs/example/g\n" +
                            "astrolabegs/gxp_load_test/g\n" +
                            "astrolabegs/indexpro_test\n" +
                            "astrolabegs/indexpro_test/g\n" +
                            "astrolabegs/load_test/g\n" +
                            "astrolabegs/my_rag_test/g\n" +
                            "astrolabegs/test/g\n" +
                            "b2bgs/b2b_test/g\n" +
                            "baikegs/lemma_test/g\n" +
                            "baikegs/test/g\n" +
                            "bdpanriskctrlgs/example/g\n" +
                            "bdpanriskctrlgs/porn_kids_v1_1/g\n" +
                            "bdpanriskctrlgs/porn_kids/g\n" +
                            "bdpanriskctrlgs/test/g\n" +
                            "bjh_project/bcp_chat_detail_2410/g\n" +
                            "bjh_project/bcp_chat_detail/g\n" +
                            "bjh_project/bcp_social_2410/g\n" +
                            "bjh_project/bcp_social/g\n" +
                            "bjh_project/bcp_test/g\n" +
                            "bjh_project/bjh_follow/g\n" +
                            "bjh_project/bjh_project_240828/g\n" +
                            "bjh_project/bjh_project_test/g\n" +
                            "bjh_project/bjh_project_ttl30/g\n" +
                            "bjh_project/bjh_project/g\n" +
                            "bjh_project/bjh_register/g\n" +
                            "bjh_project/bjh_relation/g\n" +
                            "bjh_project/bjh_social_relation/g\n" +
                            "bjh_project/bjh_social_ttl30/g\n" +
                            "bjh_project/bjh_social/g\n" +
                            "bjh_project/common_data/g\n" +
                            "comategs/comate_demo/g\n" +
                            "comategs/example/g\n" +
                            "DEFAULT/~sys_graph/g\n" +
                            "fanheichangs/benefit_g/g\n" +
                            "fanheichangs/benefit_graph/g\n" +
                            "fanheichangs/cuid2uid_graph/g\n" +
                            "fanheichangs/example/g\n" +
                            "fanheichangs/id_graph_10w/g\n" +
                            "fanheichangs/id_graph_1w/g\n" +
                            "fanheichangs/id_graph/g\n" +
                            "fanheichangs/site_graph/g\n" +
                            "fanzhags/example/g\n" +
                            "fanzhags/fengchao_id/g\n" +
                            "fengchaogs/afs_test/g\n" +
                            "fengchaogs/antiservice_gcn_min_test/g\n" +
                            "fengchaogs/antiservice_gcn_test/g\n" +
                            "fengchaogs/black_test/g\n" +
                            "fengchaogs/cvt_test/g\n" +
                            "fengkonggs/d_200003_z_i/g\n" +
                            "fengkonggs/d_200003_z_p/g\n" +
                            "fengkonggs/d_200003/g\n" +
                            "fengkonggs/d_200012_z_i/g\n" +
                            "fengkonggs/d_200012_z_p/g\n" +
                            "fengkonggs/d_200012/g\n" +
                            "fengkonggs/d_others_z_p/g\n" +
                            "fengkonggs/d_others/g\n" +
                            "fengkonggs/d_sense/g\n" +
                            "fengkonggs/dianshang_all/g\n" +
                            "fengkonggs/dianshang_b_2/g\n" +
                            "fengkonggs/dianshang_b/g\n" +
                            "fengkonggs/dianshang_group/g\n" +
                            "fengkonggs/dianshang_strong_weak/g\n" +
                            "fengkonggs/dianshang_weak/g\n" +
                            "fengkonggs/tmp/g\n" +
                            "fengkonggs/universal_graph_big/g\n" +
                            "fengkonggs/universal_graph_test/g\n" +
                            "fengkonggs/universal_graph/g\n" +
                            "fengkonggs/unsupervised_ip_graph/g\n" +
                            "graphcloudgs/automobile_knowledge_graph/g\n" +
                            "hcghealthgs/basedata/g\n" +
                            "ip_info/ip_space_time_analyse_new/g\n" +
                            "ip_info/ip_space_time_analyse_public/g\n" +
                            "ipipe/g1/g\n" +
                            "ipipe/g2/g\n" +
                            "ipipe/vermeer/g\n" +
                            "itplatformgs/eop_authz_prod/g\n" +
                            "itplatformgs/eop_authz_sandbox/g\n" +
                            "itplatformgs/example/g\n" +
                            "judicialgraphgs/graph/g\n" +
                            "judicialgraphgs/test/g\n" +
                            "judicialgraphgs/verdict/g\n" +
                            "mappoigs/map_cuid_poi_20231227/g\n" +
                            "mappoigs/mappoi_hz_0404_0406/g\n" +
                            "mappoigs/mappoi_test/g\n" +
                            "mappoigs/mappoi240318/g\n" +
                            "megqegs/code_case/g\n" +
                            "megqegs/example/g\n" +
                            "neizhianli/covid19/g\n" +
                            "neizhianli/hlm/g\n" +
                            "politicsllmgs/llm_test/g\n" +
                            "politicsllmgs/politics_llm/g\n" +
                            "searchcontentgs/example/g\n" +
                            "secaitestgs/atcp_test_1/g\n" +
                            "secaitestgs/atcp_test_2/g\n" +
                            "secaitestgs/atcp_test_3/g\n" +
                            "secaitestgs/hugegraph_server/g\n" +
                            "secaitestgs/hugegraph/g\n" +
                            "secaitestgs/rag_test/g\n" +
                            "secaitestgs/risk_sync/g\n" +
                            "secaitestgs/sec/g\n" +
                            "secaitestgs/test1/g\n" +
                            "secaitestgs/things_box/g\n" +
                            "secaitestgs/things_shield/g\n" +
                            "tieba_ronghe/social_high_percision/g\n" +
                            "tieba_ronghe/social_multi_feature/g\n" +
                            "tieba_ronghe/social_ttl_7/g\n" +
                            "tieba_ronghe/tieba_daily_cls_20240603/g\n" +
                            "tieba_ronghe/tieba_daily_cls_recover/g\n" +
                            "tieba_ronghe/tieba_daily_cls_test/g\n" +
                            "tieba_ronghe/tieba_daily_cls/g\n" +
                            "tieba_ronghe/tieba_data_0228_0304/g\n" +
                            "tieba_ronghe/tieba_data_0308_0314/g\n" +
                            "tieba_ronghe/tieba_data_24_0123_trim/g\n" +
                            "tieba_ronghe/tieba_data_24_0123/g\n" +
                            "tieba_ronghe/tieba_data_machine_learning/g\n" +
                            "tieba_ronghe/tieba_data_ttl_31/g\n" +
                            "tieba_ronghe/tieba_data_ttl_7/g\n" +
                            "tieba_ronghe/tieba_qunliao_0626_0701/g\n" +
                            "tieba_ronghe/tieba_qunliao_0701/g\n" +
                            "tieba_ronghe/tieba_qunliao_test/g\n" +
                            "tieba_ronghe/tieba_test/g\n" +
                            "tieba_ronghe/tieba_yucui/g\n" +
                            "tieba/tieba_data_fanheichan_1/g\n" +
                            "tieba/tieba_data_fanheichan_tag/g\n" +
                            "tieba/tieba_data_haotianjing/g\n" +
                            "tieba/tieba_data_new/g\n" +
                            "tieba/tieba_data/g\n" +
                            "trafficllmgs/accident_report_test/g\n" +
                            "trafficllmgs/example/g\n" +
                            "trafficllmgs/llm_rag/g\n" +
                            "trafficllmgs/nnx_traffic_event/g\n" +
                            "trafficllmgs/traffic_graph_test/g\n" +
                            "trafficllmgs/traffic_rag/g\n" +
                            "upopgs/tu_id/g\n" +
                            "wenkuraggs/demo_241112/g\n" +
                            "wenkuraggs/test_241107/g\n" +
                            "wenkuraggs/wenku_test_241112/g\n" +
                            "secaitestgs/asdfg32435r4rwewet/g";

        graphs.addAll(List.of(graphNames.split("\n")));
    }

    private final BinaryElementSerializer serializer = BinaryElementSerializer.getInstance();
    @Autowired
    private HgStoreNodeService nodeService;

    public static byte[] getShortBytes(int x) {
        byte[] buf = new byte[2];
        buf[0] = (byte) (x >> 8);
        buf[1] = (byte) (x);
        return buf;
    }

    @GetMapping(value = "/update_next_id/{partition_id}/{graph_id}", produces = "application/json")
    public String updateMaxGraphId(@PathVariable(value = "partition_id") int pid, @PathVariable(
            "graph_id") long graphId) throws IOException {
        var businessHandler = nodeService.getStoreEngine().getBusinessHandler();
        try (var manager = new GraphIdManager(businessHandler, pid)) {
            var key = MetadataKeyHelper.getCidKey(GRAPH_ID_PREFIX);
            log.info("update max graph id to {}, partition, {}", graphId, pid);
            manager.put(key, Int64Value.of(graphId));
            manager.flush();
        }
        return "OK";
    }

    @GetMapping(value = "/next_id/{partition_id}", produces = "application/json")
    public String getNextId(@PathVariable(value = "partition_id") int pid) throws IOException {
        var handler = (BusinessHandlerImpl) nodeService.getStoreEngine().getBusinessHandler();
        var op = handler.getSession(pid).sessionOp();
        var next = op.get(GraphIdManager.DEFAULT_CF_NAME,
                          MetadataKeyHelper.getCidKey(GRAPH_ID_PREFIX));
        if (next != null) {
            return String.valueOf(Int64Value.parseFrom(next).getValue());
        }
        return "NOT_FOUND";
    }

    @PostMapping(value = "/update_graph_id/{partition_id}", produces = "application/json")
    public String updateGraphId(@PathVariable(value = "partition_id") int pid,
                                @RequestBody Map<String, Long> idMap) throws IOException {
        var handler = (BusinessHandlerImpl) nodeService.getStoreEngine().getBusinessHandler();
        try (var manager = new GraphIdManager(handler, pid)) {
            idMap.forEach((graphName, graphId) -> {
                log.info("update graph id of {} to {}, partition, {}", graphName, graphId, pid);
                var graphIdKey = MetadataKeyHelper.getGraphIDKey(graphName);
                var slotKey = manager.genCIDSlotKey(GRAPH_ID_PREFIX, graphId);
                var value = Int64Value.of(graphId);
                manager.put(graphIdKey, value);
                manager.put(slotKey, value);
            });
            manager.flush();
        }
        handler.getKeyCreator().clearCache(pid);
        return "OK";
    }

    /**
     * 统计整个表中 graph id 对应对 count 以及随机抽样 100 条 (精确的数字）
     *
     * @param op    op
     * @param table table
     * @return count map and sample map
     */

    private Map.Entry<Map<Integer, Integer>, Map<Integer, List<RocksDBSession.BackendColumn>>>
    scanAndSample(SessionOperator op, String table) {
        Map<Integer, Integer> countMap = new HashMap<>();
        Map<Integer, List<RocksDBSession.BackendColumn>> sampleMap = new HashMap<>();
        Random random = new Random();

        try (var iterator = op.scan(table)) {
            while (iterator.hasNext()) {
                var col = (RocksDBSession.BackendColumn) iterator.next();
                if (col.name.length > 2) {
                    int id = (col.name[0] << 8) + (col.name[1]);
                    if (!countMap.containsKey(id)) {
                        countMap.put(id, 0);
                        sampleMap.put(id, new ArrayList<>());
                    }
                    var count = countMap.put(id, countMap.get(id) + 1);
                    if (count == null) {
                        count = 0;
                    }
                    if (count < 100) {
                        sampleMap.get(id).add(col);
                    } else {
                        int k = random.nextInt(count + 1);
                        if (k < 100) {
                            sampleMap.get(id).set(k, col);
                        }
                    }
                }
            }
        }
        return new AbstractMap.SimpleEntry<>(countMap, sampleMap);
    }

    private long getLabelId(RocksDBSession.BackendColumn col, String table) {
        BackendColumn newCol = BackendColumn.of(
                Arrays.copyOfRange(col.name, Short.BYTES, col.name.length - Short.BYTES),
                col.value);
        var id = serializer.parseLabelFromCol(newCol, Objects.equals("g+v", table));
        return id.asLong();
    }

    /**
     * 效率优化，只查前 10 万条
     *
     * @param op
     * @param table
     * @param start
     * @param end
     * @return
     */
    private Map<String, Object> scanAndSample(SessionOperator op, String table, byte[] start,
                                              byte[] end) {
        Random random = new Random();

        Set<Long> labels = new HashSet<>();
        try (var iterator = op.scan(table, start, end, ScanIterator.Trait.SCAN_LT_END)) {
            int count = 0;
            List<RocksDBSession.BackendColumn> sample = new ArrayList<>();
            while (iterator.hasNext()) {
                var col = (RocksDBSession.BackendColumn) iterator.next();
                if (col.name.length > 2) {
                    if (count < 10000 || random.nextInt(100) == 1) {
                        labels.add(getLabelId(col, table));
                    }

                    if (count < 100) {
                        sample.add(col);
                    } else {
                        int k = random.nextInt(count + 1);
                        if (k < 100) {
                            sample.set(k, col);
                        }
                    }
                    count += 1;
                }
            }
            return Map.of("count", count, "sample", sample,
                          "labels", labels.stream().map(String::valueOf)
                                          .collect(Collectors.joining(",")));

        }
    }

    /**
     * 性能优化版，按照 graph id 去扫描，根据预估文件大小，决定是否要扫这个分区
     *
     * @param session
     * @return
     */

    private Map<Integer, Map<String, Object>> scanAndSample(RocksDBSession session) {
        Map<Integer, Map<String, Object>> result = new HashMap<>();
        var op = session.sessionOp();
        for (int i = 0; i < 65536; i++) {
            var start = getShortBytes(i);
            var end = getShortBytes(i + 1);
            long size = session.getApproximateDataSize(start, end);
            if (size > 0) {
                var vMap = scanAndSample(op, "g+v", start, end);
                var eMap = scanAndSample(op, "g+ie", start, end);

                if ((int) vMap.get("count") + (int) eMap.get("count") > 0) {
                    result.put(i, Map.of("vCount", vMap.get("count"),
                                         "eCount", eMap.get("count"),
                                         "size", size,
                                         "vLabels", vMap.get("labels"),
                                         "eLabels", eMap.get("labels"),
                                         "vSample", vMap.get("sample"),
                                         "eSample", eMap.get("sample")));
                }
            }
        }
        return result;
    }

    private String elementToString(BaseElement element) {
        if (element == null) {
            return "";
        }
        StringBuilder builder = new StringBuilder();
        for (var property : element.getProperties().entrySet()) {
            BaseProperty<?> value = property.getValue();
            var v = property.getValue().value();
            if (v instanceof String) {
                builder.append(value.propertyKey().name());
                builder.append(":").append(v).append(",");
            }
        }
        return builder.toString();
    }

    private String runDeserialize(List<RocksDBSession.BackendColumn> list, boolean isVertex) {
        if (list == null || list.isEmpty()) {
            return "empty";
        }

        int total = list.size();
        StringBuilder buffer = new StringBuilder();
        for (String graph : graphs) {
            int success = 0;
            BaseElement element = null;
            for (var column : list) {
                BackendColumn newCol = BackendColumn.of(Arrays.copyOfRange(column.name, Short.BYTES,
                                                                           column.name.length -
                                                                           Short.BYTES),
                                                        column.value);
                try {
                    element = QueryUtil.parseEntry(BusinessHandlerImpl.getGraphSupplier(graph),
                                                   newCol, isVertex);
                    success++;
                } catch (Exception e) {
                }
            }
            if (success > total * 0.8) {
                buffer.append(String.format("%s: %f, %s\n", graph, success * 1.0 / total,
                                            element == null ? "FAIL" : element.toString()));
            }
        }
        return buffer.toString();
    }

    /**
     * 要同时满足能够解析定点和边
     *
     * @param list1 vertex list
     * @param list2 edge list
     * @return
     */

    private Map<String, String> runDeserialize(List<RocksDBSession.BackendColumn> list1,
                                               List<RocksDBSession.BackendColumn> list2) {
        int total1 = list1.size();
        int total2 = list2.size();
        List<String> passed = new ArrayList<>();
        BaseElement element = null;
        BaseElement element2 = null;

        for (String graph : graphs) {
            int success = 0;
            int success2 = 0;
            for (var column : list1) {
                BackendColumn newCol = BackendColumn.of(Arrays.copyOfRange(column.name, Short.BYTES,
                                                                           column.name.length -
                                                                           Short.BYTES),
                                                        column.value);
                try {
                    element = QueryUtil.parseEntry(BusinessHandlerImpl.getGraphSupplier(graph),
                                                   newCol, true);
                    success++;
                } catch (Exception e) {
                }
            }
            if (success < total1 * 0.9) {
                continue;
            }

            for (var column : list2) {
                BackendColumn newCol = BackendColumn.of(Arrays.copyOfRange(column.name, Short.BYTES,
                                                                           column.name.length -
                                                                           Short.BYTES),
                                                        column.value);
                try {
                    element2 = QueryUtil.parseEntry(BusinessHandlerImpl.getGraphSupplier(graph),
                                                    newCol, false);
                    success2++;
                } catch (Exception e) {
                }
            }

            if (success2 >= total2 * 0.9) {
                passed.add(String.format("%s:%f", graph,
                                         (success + success2) * 1.0 / (total1 + total2)));
            }
        }

        return Map.of("graphs", String.join("\n", passed), "samples",
                      String.join("\n", List.of(elementToString(element),
                                                elementToString(element2))));
    }

    private Map<Integer, String> getGraphIds(RocksDBSession session) {
        Map<Integer, String> graphs = new HashMap<>();
        var op = session.sessionOp();
        var prefix = MetadataKeyHelper.getGraphIDKey("");
        try (var iterator = op.scan(GraphIdManager.DEFAULT_CF_NAME, prefix)) {
            while (iterator.hasNext()) {
                var col = (RocksDBSession.BackendColumn) iterator.next();
                try {
                    int graphId = (int) Int64Value.parseFrom(col.value).getValue();
                    String graphName = new String(col.name).replace("HUGEGRAPH/GRAPH_ID/", "");
                    graphs.put(graphId, graphName);
                } catch (InvalidProtocolBufferException e) {
                }
            }
        }
        return graphs;
    }

    private Set<Integer> getSlotIds(RocksDBSession session) {
        Set<Integer> result = new HashSet<>();
        var op = session.sessionOp();
        var prefix = MetadataKeyHelper.getCidSlotKeyPrefix(GRAPH_ID_PREFIX);
        try (var iterator = op.scan(GraphIdManager.DEFAULT_CF_NAME, prefix)) {
            while (iterator.hasNext()) {
                var col = (RocksDBSession.BackendColumn) iterator.next();
                try {
                    int graphId = (int) Int64Value.parseFrom(col.value).getValue();
                    result.add(graphId);
                } catch (InvalidProtocolBufferException e) {
                }
            }
        }

        return result;
    }

    @GetMapping(value = "/graph_ids/{id}", produces = "application/json")
    public Map<Integer, Map<String, String>> allGraphIds(@PathVariable(value = "id") int id) {
        var session = nodeService.getStoreEngine().getBusinessHandler().getSession(id);
        var graphs = getGraphIds(session);
        var slotIds = getSlotIds(session);
        Map<Integer, Map<String, String>> result = new HashMap<>();
        for (int i = 0; i < 65536; i++) {
            var start = getShortBytes(i);
            var end = getShortBytes(i + 1);
            long size = session.getApproximateDataSize(start, end);
            long count = 0;
            if (size > 0 && size < 512) {
                count = session.sessionOp().keyCount(start, end, "g+v");
                if (count == 0) {
                    continue;
                }
            }
            if (size > 0 || graphs.containsKey(i)) {
                Map<String, String> tmp = new HashMap<>();
                tmp.put("size", String.valueOf(size));
                tmp.put("graph", graphs.getOrDefault(i, "not found"));
                if (count > 0) {
                    tmp.put("count", String.valueOf(count));
                }
                if (slotIds.contains(i)) {
                    tmp.put("has_slot_id", "true");
                }
                result.put(i, tmp);
            }
        }
        return result;
    }

    @GetMapping(value = "/check/{id}", produces = "application/json")
    public Map<Integer, Map<String, String>> checkGraphId(@PathVariable(value = "id") int id) {
        var businessHandler = nodeService.getStoreEngine().getBusinessHandler();
        var session = businessHandler.getSession(id);
        Map<Integer, String> graphs = getGraphIds(session);

        var result = new HashMap<Integer, Map<String, String>>();
        var samples = scanAndSample(session);

        for (var entry : samples.entrySet()) {
            var graphId = entry.getKey();
            var value = entry.getValue();

            Map<String, String> map = new HashMap<>();
            map.put("size", String.valueOf(value.get("size")));
            map.put("vertex count", String.valueOf(value.get("vCount")));
            map.put("in edge count", String.valueOf(value.get("eCount")));
            map.put("graph id", graphs.getOrDefault(graphId, "not found"));
            map.put("vLabels", String.valueOf(value.get("vLabels")));
            map.put("eLabels", String.valueOf(value.get("eLabels")));

            var list1 = (List<RocksDBSession.BackendColumn>) value.get("vSample");
            var list2 = (List<RocksDBSession.BackendColumn>) value.get("eSample");

            var parseResult = runDeserialize(list1, list2);
            map.put("graphs", parseResult.getOrDefault("graphs", ""));
            map.put("samples", parseResult.getOrDefault("samples", ""));
            result.put(graphId, map);
        }
        return result;
    }

    @GetMapping(value = "/delete_graph_id/{partition}/{graph_id}", produces = "application/json")
    public String deleteGraphId(@PathVariable(value = "partition") int pid,
                                @PathVariable("graph_id") int gid) {
        byte[] start = getShortBytes(gid);
        byte[] end = Arrays.copyOf(start, start.length);
        increaseOne(end);
        var businessHandler = nodeService.getStoreEngine().getBusinessHandler();

        var op = businessHandler.getSession(pid).sessionOp();
        var tables = List.of("g+v", "g+ie", "g+oe", "g+index", "g+olap");
        for (var table : tables) {
            op.deleteRange(table, start, end);
        }
        return "OK";
    }

    @GetMapping(value = "/clean/{graph:.+}", produces = "application/json")
    public String cleanGraph(@PathVariable(value = "graph") String graph) {
        var businessHandler = nodeService.getStoreEngine().getBusinessHandler();
        var tables = List.of("g+v", "g+ie", "g+oe");

        InnerKeyCreator keyCreator = new InnerKeyCreator(businessHandler);
        var supplier = BusinessHandlerImpl.getGraphSupplier(graph);

        var partitions = businessHandler.getPartitionIds(graph);
        for (var pid : partitions) {
            var session = businessHandler.getSession(pid);
            var op = session.sessionOp();

            for (String table : tables) {
                boolean isVertex = QueryUtil.isVertex(table);
                try (var itr = op.scan(table, keyCreator.getStartKey(pid, graph),
                                       keyCreator.getEndKey(pid, graph), 0)) {
                    while (itr.hasNext()) {
                        var col = (RocksDBSession.BackendColumn) itr.next();
                        BackendColumn newCol = BackendColumn.of(
                                Arrays.copyOfRange(col.name, Short.BYTES,
                                                   col.name.length - Short.BYTES), col.value);
                        try {
                            QueryUtil.parseEntry(supplier, newCol, isVertex);
                        } catch (Exception e) {
                            op.delete(table, col.name);
                        }
                    }
                }
            }
            op.commit();
        }

        return "OK";
    }
}
