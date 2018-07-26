package com.data.task.pipeline.server.beans;

import com.data.task.pipeline.core.beans.TaskPipelineAppTaskListener;
import com.data.task.pipeline.core.beans.TaskPipelineAssignTaskStatusListener;
import com.data.task.pipeline.core.beans.TaskPipelineCoreConfig;
import com.data.task.pipeline.core.beans.TaskPipelineOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author xinzai
 * @create 2018-07-25 下午4:36
 **/
public class TaskPipelineServerOperation extends TaskPipelineOperation {

    private static Logger log = LoggerFactory.getLogger(TaskPipelineServerOperation.class);

    public TaskPipelineServerOperation(TaskPipelineCoreConfig config) {
        super(config);
    }

    public void initTaskWatcher(TaskPipelineAppTaskListener listener) throws Exception {
        List<String> apps = getTaskAppList();
        apps.forEach(appName -> {
            try {
                watchTaskList(appName, listener);
            } catch (Exception e) {
                log.error("watch app:{} task list exception:{}",appName,e);
            }
        });
    }

    public void assignTaskAndWatchStatus(String appName, String taskName, String worker, TaskPipelineAssignTaskStatusListener listener) throws Exception {
        assignTask(appName,taskName, worker);
        watchAssignTaskStatus(appName,taskName,worker,listener);
    }


}
