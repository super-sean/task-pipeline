package com.data.task.pipeline.core.beans.listener;

import com.data.task.pipeline.core.beans.TaskPipelineCoreConstant;
import com.data.task.pipeline.core.beans.operation.TaskPipelineOperation;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.APP;
import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.SERVER;

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
    private String platform;
    private String result;

    public TaskPipelineTaskStatusListener(String platform,String appName) {
        this.platform = platform;
        this.appName = appName;
        listener = () -> {
            if(cache.getCurrentData() == null){
                log.info("app:{} task:{} change",appName,taskName);
                return;
            }
            String data = new String(cache.getCurrentData().getData());
            log.info("app:{} task:{} change data:{}",appName,taskName,data);
            onTaskStatusChangeCallback(appName,taskName,data);
        };
    }

    /**
     * 获取回调并标记任务状态为已消费
     * @param appName
     * @param taskName
     * @param status
     */
    private void onTaskStatusChangeCallback(String appName,String taskName,String status) throws Exception {
        boolean isAppDone = TaskPipelineCoreConstant.TaskStatus.DONE.status().equals(status) || TaskPipelineCoreConstant.TaskStatus.NOWORKER.status().equals(status);
        if(APP.equals(platform) && isAppDone) {
            operation.updateTaskStatus(appName, taskName, TaskPipelineCoreConstant.TaskStatus.CONSUMED.status());
            storeResult();
            shutdown();
        }
        boolean isServerDone = TaskPipelineCoreConstant.TaskStatus.CONSUMED.status().equals(status) || TaskPipelineCoreConstant.TaskStatus.MISSAPP.status().equals(status);
        if(SERVER.equals(platform) && isServerDone) {
            shutdown();
        }
        onTaskStatusChange(appName,taskName,status);
    }

    private void storeResult(){
        try {
            if(!operation.checkTaskResultExist(appName,taskName)){
                result = "";
                return;
            }
            result = operation.getTaskResult(appName,taskName);
        } catch (Exception e) {
            log.error("task pipeline app:{} get task:{} exception:{}",appName,taskName,e);
        }
    }

    public String getTaskResult() {
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

    public void setOperation(TaskPipelineOperation operation) {
        this.operation = operation;
    }

    public void setTaskName(String taskName){
        this.taskName = taskName;
    }

    public String getTaskName() {
        return taskName;
    }

    public void shutdown(){
        try {
            operation.removeListener(cache,listener);
            operation = null;
            listener = null;
            cache = null;
        } catch (IOException e) {
            log.error("task status listener app:{} task:{} remove exception",appName,taskName,e);
        }
    }
}
