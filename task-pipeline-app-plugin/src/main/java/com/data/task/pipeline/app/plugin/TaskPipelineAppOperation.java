package com.data.task.pipeline.app.plugin;

import com.data.task.pipeline.core.beans.TaskPipelineCoreConfig;
import com.data.task.pipeline.core.beans.operation.TaskPipelineOperation;
import com.data.task.pipeline.core.beans.listener.TaskPipelineTaskStatusListener;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.TASKS_PATH;

/**
 * @author xinzai
 * @create 2018-07-24 下午3:27
 **/
public class TaskPipelineAppOperation extends TaskPipelineOperation {
    private String appName;

    public TaskPipelineAppOperation(String appName,TaskPipelineCoreConfig config) {
        super(config);
        this.appName = appName;
    }

    public void registerApp(String node) throws Exception {
        registerAppNode(appName,node);
        if (!checkNodeExist(TASKS_PATH + appName)) {
             createNode(TASKS_PATH + appName,"");
        }
    }

    public void submitTask(String taskName,String params,TaskPipelineTaskStatusListener listener) throws Exception {
        submitTaskNode(appName,taskName,params);
        watchTaskStatus(appName,taskName,listener);
    }

}
