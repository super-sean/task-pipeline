package com.data.task.pipeline.server.beans;

import com.data.task.pipeline.core.beans.*;
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
        //监听task app目录下的新节点变化
        apps.forEach(appName -> {
            try {
                watchTaskList(appName, listener);
            } catch (Exception e) {
                log.error("watch app:{} task list exception:{}",appName,e);
            }
        });

    }

    public void assignTaskAndWatchStatus(String appName, String taskName, String worker, TaskPipelineTaskStatusListener taskStatusListener, TaskPipelineAssignTaskStatusListener assignTaskStatusListener) throws Exception {
        assignTask(appName,taskName, worker);
        watchTaskStatus(appName,taskName,taskStatusListener);
        watchAssignTaskStatus(appName,taskName,worker,assignTaskStatusListener);
    }


}
