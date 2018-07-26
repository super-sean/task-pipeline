package com.data.task.pipeline.server.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author xinzai
 * @create 2018-07-25 下午4:26
 **/

@ConfigurationProperties(prefix = "task.pipeline")
public class TaskPipelineServerConfig {
    private String zkConnectStr;

    private int sessionTimeout;

    private int baseSleepTimeMs;

    private int maxRetries;

    private int corePoolSize;

    private int maxthreadPoolSize;

    private int keepApiveTime;

    private int queueSize;

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
}
