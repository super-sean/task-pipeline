package com.data.task.pipeline.core.beans;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xinzai
 * @create 2018-07-24 下午5:13
 **/
public abstract class TaskPipelineTaskStatusListener {
    private static Logger log = LoggerFactory.getLogger(TaskPipelineTaskStatusListener.class);
    private String appName;
    private String taskName;
    private NodeCache cache;
    private NodeCacheListener listener;
    private TaskPipelineOperation operation;

    public TaskPipelineTaskStatusListener(String appName) {
        this.appName = appName;
        listener = () -> {
            if(cache.getCurrentData() == null){
                return;
            }
            onTaskStatusChangeCallback(appName,taskName,new String(cache.getCurrentData().getData()));
        };
    }

    /**
     * 获取回调并标记任务状态为已消费
     * @param appName
     * @param taskName
     * @param status
     */
    private void onTaskStatusChangeCallback(String appName,String taskName,String status) throws Exception {
        if(TaskPipelineCoreConstant.TaskStatus.DONE.status().equals(status)) {
            operation.updateTaskStatus(appName, taskName, TaskPipelineCoreConstant.TaskStatus.CONSUMED.status());
        }
        onTaskStatusChange(appName,taskName,status);
    }

    public String getTaskResult() {
        String result = "";
        try {
            if(!operation.checkTaskResultExist(appName,taskName)){
                return result;
            }
            result = operation.getTaskResult(appName,taskName);
        } catch (Exception e) {
            log.error("task pipeline app:{} get task:{} exception:{}",appName,taskName,e);
        }
        return result;
    }
    /**
     * 用于业务端实现任务状态变化时回调
     * @param appName
     * @param taskName
     * @param status
     */
    public abstract void onTaskStatusChange(String appName,String taskName,String status);

    public NodeCache getCache() {
        return cache;
    }

    public void setCache(NodeCache cache) {
        this.cache = cache;
    }

    public NodeCacheListener getListener() {
        return listener;
    }

    protected void setOperation(TaskPipelineOperation operation) {
        this.operation = operation;
    }

    public void setTaskName(String taskName){
        this.taskName = taskName;
    }

    public String getTaskName() {
        return taskName;
    }
}
