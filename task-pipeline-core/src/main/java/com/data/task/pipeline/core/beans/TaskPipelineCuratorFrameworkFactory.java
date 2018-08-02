package com.data.task.pipeline.core.beans;

import com.data.task.pipeline.core.beans.config.TaskPipelineACLProvider;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import javax.annotation.PreDestroy;

/**
 * @author xinzai
 * create 2018-07-23 下午3:01
 **/
public class TaskPipelineCuratorFrameworkFactory {

    private String nameSpace;

    private String zkConnectStr;

    private int sessionTimeout;

    //间隔时间重试
    private int baseSleepTimeMs = 1000;

    //重试次数
    private int maxRetries = 0;

    private CuratorFramework cf;

    public TaskPipelineCuratorFrameworkFactory(String nameSpace, String zkConnectStr, int sessionTimeout, int baseSleepTimeMs, int maxRetries, TaskPipelineACLProvider aCLProvider) {
        this.nameSpace = nameSpace;
        this.zkConnectStr = zkConnectStr;
        this.sessionTimeout = sessionTimeout;

        if(baseSleepTimeMs > 0){
            this.baseSleepTimeMs = baseSleepTimeMs;
        }

        if(maxRetries > 0) {
            this.maxRetries = maxRetries;
        }
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries);
        cf = CuratorFrameworkFactory.builder()
                .aclProvider(aCLProvider)
                .authorization(aCLProvider.getAuthInfos())
                .connectString(zkConnectStr)
                .sessionTimeoutMs(sessionTimeout)
                .retryPolicy(retryPolicy)
                .namespace(nameSpace)
                .build();
    }

    public CuratorFramework getCuratorFramework() {
        return cf;
    }


    @PreDestroy
    public void destroy(){
        cf.close();
    }


    public String getNameSpace() {
        return nameSpace;
    }

    public String getZkConnectStr() {
        return zkConnectStr;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public int getBaseSleepTimeMs() {
        return baseSleepTimeMs;
    }

    public int getMaxRetries() {
        return maxRetries;
    }
}
