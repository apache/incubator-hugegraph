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

package com.baidu.hugegraph.unit.core;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.baidu.hugegraph.testutil.Assert;

import com.baidu.hugegraph.analyzer.Analyzer;
import com.baidu.hugegraph.analyzer.AnalyzerFactory;

public class AnalyzerTest {

    private static String text1 = "England wins World Cup";
    private static String text2 = "英格兰世界杯夺冠，中华人民共和国国歌，" +
                                  "百度科技园位于北京市海淀区西北旺东路10号院";

    @Before
    public void setup() {
        // pass
    }

    @After
    public void teardown() {
        // pass
    }

    @Test
    public void testWordAnalyzer() {
        // MaximumMatching mode
        Analyzer analyzer = AnalyzerFactory.analyzer("word", "MaximumMatching");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北旺", "东路", "10号", "院"),
                            analyzer.segment(text2));

        // ReverseMaximumMatching mode
        analyzer = AnalyzerFactory.analyzer("word", "ReverseMaximumMatching");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北旺", "东路", "10号", "院"),
                            analyzer.segment(text2));
    }

    @Test
    public void testStanfordAnalyzer() {
        // StanfordCoreNLP-chinese-ctb mode
        Analyzer analyzer = AnalyzerFactory.analyzer("stanford", "StanfordCoreNLP-chinese-ctb");
        Assert.assertEquals(setOf("England", "wins", "World", "Cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华", "人民",
                                  "共和国", "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10号", "院"),
                            analyzer.segment(text2));

        // StanfordCoreNLP-chinese-pku mode
        analyzer = AnalyzerFactory.analyzer("stanford", "StanfordCoreNLP-chinese-pku");
        Assert.assertEquals(setOf("England", "wins", "World", "Cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华人民",
                                  "共和国", "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10号", "院"),
                            analyzer.segment(text2));
    }

    @Test
    public void testAnsjAnalyzer() {
        // BaseAnalysis mode
        Analyzer analyzer = AnalyzerFactory.analyzer("ansj", "BaseAnalysis");
        Assert.assertEquals(setOf("england", " ", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华人民共和国",
                                  "国歌", "百度", "科技", "园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10", "号", "院"),
                            analyzer.segment(text2));

        // IndexAnalysis mode
        analyzer = AnalyzerFactory.analyzer("ansj", "IndexAnalysis");
        Assert.assertEquals(setOf("england", " ", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华人民共和国",
                                  "国歌", "百度", "科技", "园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10号", "院"),
                            analyzer.segment(text2));
    }

    @Test
    public void testHanlpAnalyzer() {
        // standard mode
        Analyzer analyzer = AnalyzerFactory.analyzer("hanlp", "standard");
        Assert.assertEquals(setOf("England", " ", "wins", "World", "Cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北旺", "东路", "10", "号", "院"),
                            analyzer.segment(text2));

        // nlp mode
        analyzer = AnalyzerFactory.analyzer("hanlp", "nlp");
        Assert.assertEquals(setOf("England", " ", "wins", "World", "Cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北旺", "东路10号院"),
                            analyzer.segment(text2));
    }

    @Test
    public void testSmartCNAnalyzer() {
        Analyzer analyzer = AnalyzerFactory.analyzer("smartcn", "");
        Assert.assertEquals(setOf("england", "win", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "中华人民共和国",
                                  "国歌", "百", "度", "科技", "园", "位于",
                                  "北京市", "海淀区", "西北", "旺", "东", "路",
                                  "10", "号", "院"),
                            analyzer.segment(text2));
    }

    @Test
    public void testJiebaAnalyzer() {
        // SEARCH mode
        Analyzer analyzer = AnalyzerFactory.analyzer("jieba", "SEARCH");
        Assert.assertEquals(setOf("england", " ", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10", "号院"),
                            analyzer.segment(text2));

        // INDEX mode
        analyzer = AnalyzerFactory.analyzer("jieba", "INDEX");
        Assert.assertEquals(setOf("england", " ", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界", "世界杯", "夺冠", "，", "中华",
                                  "华人", "人民", "共和", "共和国", "中华人民共和国",
                                  "国歌", "百度", "科技", "科技园", "位于", "北京",
                                  "京市", "北京市", "海淀", "淀区", "海淀区", "西北",
                                  "旺", "东路", "10", "号院"),
                            analyzer.segment(text2));
    }

    @Test
    public void testJcsegAnalyzer() {
        // Simple mode
        Analyzer analyzer = AnalyzerFactory.analyzer("jcseg", "Simple");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华",
                                  "人民共和国", "国歌", "百度", "科技", "园", "位于",
                                  "北京市", "海淀区", "西北", "旺", "东路", "10",
                                  "号", "院"),
                            analyzer.segment(text2));

        // Complex mode
        analyzer = AnalyzerFactory.analyzer("jcseg", "Complex");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "，", "中华",
                                  "人民共和国", "国歌", "百度", "科技", "园", "位于",
                                  "北京市", "海淀区", "西北", "旺", "东路", "10",
                                  "号", "院"),
                            analyzer.segment(text2));
    }

    @Test
    public void testMMSeg4JAnalyzer() {
        // Simple mode
        Analyzer analyzer = AnalyzerFactory.analyzer("mmseg4j", "Simple");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10", "号",
                                  "院"),
                            analyzer.segment(text2));

        // Complex mode
        analyzer = AnalyzerFactory.analyzer("mmseg4j", "Complex");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10", "号",
                                  "院"),
                            analyzer.segment(text2));
    }

    @Test
    public void testIKAnalyzer() {
        // Smart mode
        Analyzer analyzer = AnalyzerFactory.analyzer("ikanalyzer", "smart");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "夺冠", "中华人民共和国",
                                  "国歌", "百度", "科技园", "位于", "北京市",
                                  "海淀区", "西北", "旺", "东路", "10号", "院"),
                            analyzer.segment(text2));

        // Max_word mode
        analyzer = AnalyzerFactory.analyzer("ikanalyzer", "max_word");
        Assert.assertEquals(setOf("england", "wins", "world", "cup"),
                            analyzer.segment(text1));
        Assert.assertEquals(setOf("英格兰", "世界杯", "世界", "杯", "夺冠",
                                  "中华人民共和国", "中华人民", "中华", "华人",
                                  "人民共和国", "人民", "共和国", "共和", "国",
                                  "国歌", "百度", "百", "度", "科技园", "科技",
                                  "园", "位于", "北京市", "北京", "市", "海淀区",
                                  "海淀", "淀区", "西北", "旺", "东路", "10",
                                  "号", "院"),
                            analyzer.segment(text2));
    }

    private static Set<String> setOf(String... elems) {
        Set<String> result = new HashSet<>();
        for (String elem : elems) {
            result.add(elem);
        }
        return result;
    }

}
