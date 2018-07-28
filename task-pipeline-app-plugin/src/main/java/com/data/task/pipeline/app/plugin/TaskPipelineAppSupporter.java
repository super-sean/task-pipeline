package com.data.task.pipeline.app.plugin;

import com.data.task.pipeline.core.beans.config.TaskPipelineCoreConfig;
import com.data.task.pipeline.core.beans.TaskPipelineUtils;
import com.data.task.pipeline.core.beans.listener.TaskPipelineTaskStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xinzai
 * @create 2018-07-23 下午2:17
 **/
public class TaskPipelineAppSupporter {

    private static Logger log = LoggerFactory.getLogger(TaskPipelineAppSupporter.class);

    private String appName;

    private TaskPipelineAppOperation operation;

    private String nodeName;


    public TaskPipelineAppSupporter(String appName,TaskPipelineCoreConfig config) {
        this.appName = appName;
        operation = new TaskPipelineAppOperation(appName,config);
        nodeName = TaskPipelineUtils.getLocalNodeName();
        try {
            operation.registerApp(nodeName);
        } catch (Exception e) {
            log.error("register to task-pipeline platform exception",e);
        }
    }

    public void submitTask(String params,TaskPipelineTaskStatusListener listener) throws Exception {
        String taskName = operation.genericTaskName(nodeName);
        operation.submitTask(taskName,params,listener);
    }

    public String getTaskStatus(String taskName) throws Exception {
        return operation.getTaskStatus(appName,taskName);
    }

    public String getTaskResult(String taskName) throws Exception {
        return operation.getTaskResult(appName,taskName);
    }

    public String getAppName() {
        return appName;
    }

    public String getNodeName() {
        return nodeName;
    }
}
