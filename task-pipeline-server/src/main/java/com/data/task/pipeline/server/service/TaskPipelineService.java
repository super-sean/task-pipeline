package com.data.task.pipeline.server.service;

import com.data.task.pipeline.core.beans.*;
import com.data.task.pipeline.server.beans.TaskPipelineServerOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import static java.lang.Thread.sleep;

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
            public void onAppTaskSubmit(String appName, String taskName) {
                assignTask(appName,taskName);
                watchTaskStatusAndArchive(appName,taskName);
            }
        });
    }

    private void watchTaskStatusAndArchive(String appName, String taskName){
        try {
            operation.watchTaskStatus(appName, taskName, new TaskPipelineTaskStatusListener(appName) {
                @Override
                public void onTaskStatusChange(String appName, String taskName, String status) {
                    judgeAndArchiveTask(appName,taskName,status);
                }
            });
        } catch (Exception e){
            log.warn("watchTaskStatus appName:{} taskName:{} exception:{}",appName,taskName,e);
        }
    }

    private void judgeAndArchiveTask(String appName, String taskName,String status){
        if(!TaskPipelineCoreConstant.TaskStatus.CONSUMED.status().equals(status)) {
            return;
        }

        try {
            operation.archiveTask(appName,taskName);
        } catch (Exception e) {
            log.error("archiveTask appName:{} taskName:{} exception:{}",appName,taskName,e);
        }
    }

    private void assignTask(String appName, String taskName){
        Map<String,String> worker = new WeakHashMap<>();
        try {
            worker = getWorker(operation, appName);
            if(StringUtils.isEmpty(worker.get("node"))){
                log.warn("no worker for app:{} task:{}",appName,taskName);
                noWorkerAssignResponse(operation,appName,taskName);
                return;
            }
            operation.assignTaskAndWatchStatus(appName,taskName, worker.get("node"), new TaskPipelineAssignTaskStatusListener(appName,taskName) {
                @Override
                public void onAssignTaskDone(String appName, String assignTaskName) {
                    archiveAssignTask(appName,assignTaskName);
                }
            });
            operation.updateWorkerWeight(appName,worker.get("node"),(Integer.parseInt(worker.get("weight")) + 1) + "");
        } catch (Exception e) {
            log.error("assign worker:{} for app:{} task:{} exception:{}", worker,appName,taskName,e);
        }
    }

    private void archiveAssignTask(String appName, String assignTaskName){
        try {
            operation.archiveAssignTask(appName,assignTaskName);
        } catch (Exception e) {
            log.warn("archive app:{} assign Task:{} exception:{}",appName,assignTaskName,e);
        }
    }

    private Map<String,String> getWorker(TaskPipelineOperation operation, String appName) throws Exception {
        List<String> workers = operation.getWorkerList(appName);
        //间隔10秒重新获取一次
        if(workers.size() == 0){
            sleep(10000);
            workers = operation.getWorkerList(appName);
        }
        final int[] weight = {Integer.MAX_VALUE};
        final String[] workerNode = {""};
        Map<String,String> worker = new WeakHashMap<>();
        //获取权重最小的第一个节点作为worker
        workers.forEach(w -> {
            try {
                int tmpWeight = operation.getWorkerWeight(appName,w);
                if(tmpWeight >= weight[0]) {
                    return;
                }
                weight[0] = tmpWeight;
                workerNode[0] = w;
                worker.put("weight",weight[0] + "");
                worker.put("node",workerNode[0]);
            } catch (Exception e) {
                log.error("get app:{} worker to assign exception:{}",appName,e);
            }
        });
        return worker;
    }

    private void noWorkerAssignResponse(TaskPipelineOperation operation,String appName,String taskName) throws Exception {
        operation.updateTaskStatus(appName,taskName, TaskPipelineCoreConstant.TaskStatus.NOWORKER.status());
    }

}
