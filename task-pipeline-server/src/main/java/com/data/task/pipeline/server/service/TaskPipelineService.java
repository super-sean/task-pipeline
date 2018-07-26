package com.data.task.pipeline.server.service;

import com.data.task.pipeline.core.beans.TaskPipelineAppTaskListener;
import com.data.task.pipeline.server.beans.TaskPipelineServerOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.List;

/**
 * @author xinzai
 * @create 2018-07-25 下午4:58
 **/
@Service
public class TaskPipelineService {
    private static Logger log = LoggerFactory.getLogger(TaskPipelineService.class);

    @Autowired
    private TaskPipelineServerOperation operation;

    @PostConstruct
    public void gernericTaskPipelineAction() throws Exception {
        operation.initTaskWatcher(new TaskPipelineAppTaskListener() {
            @Override
            public void onAppTaskChange(String appName, String taskName) {
                List<String> workers = getWorkers(appName);
                if(workers.size() == 0){
                    log.warn("no worker for app:{} task:{}",appName,taskName);
                }
                String workerNode = workers.get(0);
                try {
                    getOperation().assignTask(appName,taskName,workerNode);
                } catch (Exception e) {
                    log.error("assign worker:{} for app:{} task:{} exception:{}",workerNode,appName,taskName,e);
                }
            }
        });
    }

    private List<String> getWorkers(String appName){
        List<String> workers = Collections.emptyList();
        try {
            workers = operation.getWorkerList(appName);
        } catch (Exception e) {
            log.error("get app:{} worker list exception:{}",appName,e);
        }
        return workers;
    }

}
