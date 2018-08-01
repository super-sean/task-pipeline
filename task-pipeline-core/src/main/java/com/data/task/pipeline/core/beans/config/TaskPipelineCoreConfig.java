package com.data.task.pipeline.core.beans.config;

import java.util.Collections;
import java.util.List;

/**
 * @author xinzai
 * @create 2018-07-24 上午10:01
 **/
public class TaskPipelineCoreConfig {


    private String zkConnectStr;

    private int sessionTimeout;

    private int baseSleepTimeMs;

    private int maxRetries;

    private int corePoolSize;

    private int maxthreadPoolSize;

    private int keepApiveTime;

    private int queueSize;

    private List<String> aclIds = Collections.emptyList();

    private String aclId;

    public String getZkConnectStr() {
        return zkConnectStr;
    }

    public void setZkConnectStr(String zkConnectStr) {
        this.zkConnectStr = zkConnectStr;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public int getBaseSleepTimeMs() {
        return baseSleepTimeMs;
    }

    public void setBaseSleepTimeMs(int baseSleepTimeMs) {
        this.baseSleepTimeMs = baseSleepTimeMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public void setCorePoolSize(int corePoolSize) {
        this.corePoolSize = corePoolSize;
    }

    public int getMaxthreadPoolSize() {
        return maxthreadPoolSize;
    }

    public void setMaxthreadPoolSize(int maxthreadPoolSize) {
        this.maxthreadPoolSize = maxthreadPoolSize;
    }

    public int getKeepApiveTime() {
        return keepApiveTime;
    }

    public void setKeepApiveTime(int keepApiveTime) {
        this.keepApiveTime = keepApiveTime;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public List<String> getAclIds() {
        return aclIds;
    }

    public void setAclIds(List<String> aclIds) {
        this.aclIds = aclIds;
    }

    public String getAclId() {
        return aclId;
    }

    public void setAclId(String aclId) {
        this.aclId = aclId;
    }
}
