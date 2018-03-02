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

package com.baidu.hugegraph.backend.store.palo;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

public class PaloLoadInfo {

    private long jobId;
    private String label;
    private State state;
    private String progress;
    private String etlInfo;
    private String taskInfo;
    private String errorMsg;
    private Date createTime;
    private Date etlStartTime;
    private Date etlFinishTime;
    private Date loadStartTime;
    private Date loadFinishTime;
    private String url;

    public PaloLoadInfo(ResultSet result) throws SQLException {
        this.jobId = result.getLong("JobId");
        this.label = result.getString("Label");
        this.state = PaloLoadInfo.State.valueOf(result.getString("State"));
        this.progress = result.getString("Progress");
        this.etlInfo = result.getString("EtlInfo");
        this.taskInfo = result.getString("TaskInfo");
        this.errorMsg = result.getString("ErrorMsg");
        this.createTime = result.getDate("CreateTime");
        this.etlStartTime = result.getDate("EtlStartTime");
        this.etlFinishTime = result.getDate("EtlFinishTime");
        this.loadStartTime = result.getDate("LoadStartTime");
        this.loadFinishTime = result.getDate("LoadFinishTime");
        this.url = result.getString("URL");
    }

    public long getJobId() {
        return this.jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public String getLabel() {
        return this.label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public State getState() {
        return this.state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public void setState(String state) {
        this.state = PaloLoadInfo.State.valueOf(state);
    }

    public String getProgress() {
        return this.progress;
    }

    public void setProgress(String progress) {
        this.progress = progress;
    }

    public String getEtlInfo() {
        return this.etlInfo;
    }

    public void setEtlInfo(String etlInfo) {
        this.etlInfo = etlInfo;
    }

    public String getTaskInfo() {
        return this.taskInfo;
    }

    public void setTaskInfo(String taskInfo) {
        this.taskInfo = taskInfo;
    }

    public String getErrorMsg() {
        return this.errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public Date getCreateTime() {
        return this.createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getEtlStartTime() {
        return this.etlStartTime;
    }

    public void setEtlStartTime(Date etlStartTime) {
        this.etlStartTime = etlStartTime;
    }

    public Date getEtlFinishTime() {
        return this.etlFinishTime;
    }

    public void setEtlFinishTime(Date etlFinishTime) {
        this.etlFinishTime = etlFinishTime;
    }

    public Date getLoadStartTime() {
        return this.loadStartTime;
    }

    public void setLoadStartTime(Date loadStartTime) {
        this.loadStartTime = loadStartTime;
    }

    public Date getLoadFinishTime() {
        return this.loadFinishTime;
    }

    public void setLoadFinishTime(Date loadFinishTime) {
        this.loadFinishTime = loadFinishTime;
    }

    public String getUrl() {
        return this.url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public enum State {

        UNKNOWN(0, "UNKNOWN"),
        PENDING(1, "PENDING"),
        ETL(2, "ETL"),
        LOADING(3, "LOADING"),
        FINISHED(4, "FINISHED"),
        CANCELLED(5, "CANCELLED");

        private byte code;
        private String name;

        private State(int code, String name) {
            assert code >= 0 && code < 256;
            this.code = (byte) code;
            this.name = name;
        }

        public byte code() {
            return this.code;
        }

        public String string() {
            return this.name;
        }
    }
}
