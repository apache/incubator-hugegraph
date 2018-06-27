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

package com.baidu.hugegraph.analyzer;

import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.config.ConfigException;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.google.common.collect.ImmutableList;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 * Reference from https://my.oschina.net/apdplat/blog/412921
 */
public class StanfordAnalyzer implements Analyzer {

    public static final List<String> SUPPORT_MODES = ImmutableList.of(
            "StanfordCoreNLP-chinese-ctb",
            "StanfordCoreNLP-chinese-pku"
    );

    private StanfordCoreNLP coreNLP;

    public StanfordAnalyzer(String mode) {
        try {
            this.coreNLP = new StanfordCoreNLP(mode);
        } catch (Exception e) {
            throw new ConfigException(
                      "Unsupported segment mode '%s' for stanford analyzer, " +
                      "the available values are %s", e, mode, SUPPORT_MODES);
        }
    }

    @Override
    public Set<String> segment(String text) {
        Annotation document = new Annotation(text);
        this.coreNLP.annotate(document);
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);
        Set<String> result = InsertionOrderUtil.newSet();
        for (CoreMap sentence : sentences) {
            for (CoreLabel token : sentence.get(TokensAnnotation.class)) {
                String word = token.get(TextAnnotation.class);
                result.add(word);
            }
        }
        return result;
    }
}
