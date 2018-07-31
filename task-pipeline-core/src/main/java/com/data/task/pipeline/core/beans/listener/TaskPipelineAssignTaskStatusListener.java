package com.data.task.pipeline.core.beans.listener;

import com.data.task.pipeline.core.beans.TaskPipelineCoreConstant;
import com.data.task.pipeline.core.beans.operation.TaskPipelineOperation;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author xinzai
 * @create 2018-07-24 下午5:13
 **/
public abstract class TaskPipelineAssignTaskStatusListener {
    private static Logger log = LoggerFactory.getLogger(TaskPipelineAssignTaskStatusListener.class);
    private String appName;
    private String taskName;
    private String assignTaskName;
    private NodeCache cache;
    private NodeCacheListener listener;
    private TaskPipelineOperation operation;

    public TaskPipelineAssignTaskStatusListener(String appName,String taskName) {
        this.appName = appName;
        this.taskName = taskName;
        listener = () -> {
            if(cache.getCurrentData() == null){
                return;
            }
            onAssignTaskStatusChangeCallback(appName,assignTaskName,new String(cache.getCurrentData().getData()));
        };
    }

    /**
     * 获取回调并标记作业状态为已完成
     * @param appName
     * @param assignTaskName
     * @param status
     */
    private void onAssignTaskStatusChangeCallback(String appName,String assignTaskName,String status) throws Exception {
        if(!TaskPipelineCoreConstant.TaskStatus.DONE.status().equals(status)) {
            return;
        }
        shutdown();
        onAssignTaskDone(appName,assignTaskName);
    }

    /**
     * 用于服务端实现作业状态变化时回调
     * @param appName
     * @param assignTaskName
     */
    public abstract void onAssignTaskDone(String appName,String assignTaskName);

    public NodeCache getCache() {
        return cache;
    }

    public void setCache(NodeCache cache) {
        this.cache = cache;
    }

    public NodeCacheListener getListener() {
        return listener;
    }

    public void setOperation(TaskPipelineOperation operation) {
        this.operation = operation;
    }

    public String getAssignTaskName() {
        return assignTaskName;
    }

    public void setAssignTaskName(String assignTaskName) {
        this.assignTaskName = assignTaskName;
    }

    public void shutdown(){
        try {
            operation.removeListener(cache,listener);
            operation = null;
            listener = null;
            cache = null;
        } catch (IOException e) {
            log.error("assign task status listener app:{} task:{} assignTaskName:{} remove exception",appName,taskName,assignTaskName,e);
        }
    }
}
