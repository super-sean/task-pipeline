package com.data.task.pipeline.worker.plugin;

import com.data.task.pipeline.core.beans.TaskPipelineCoreConfig;
import com.data.task.pipeline.core.beans.operation.TaskPipelineOperation;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.ASSIGN_PATH;

/**
 * @author xinzai
 * @create 2018-07-25 上午10:31
 **/
public class TaskPipelineWorkerOperation extends TaskPipelineOperation {

    private String appName;

    public TaskPipelineWorkerOperation(String appName,TaskPipelineCoreConfig config) {
        super(config);
        this.appName = appName;
    }

    public void registerWorker(String node) throws Exception {
        registerWorkerNode(appName,node);
        if (!checkNodeExist(ASSIGN_PATH + appName)) {
            createNode(ASSIGN_PATH + appName,"");
        }
    }

}
