package com.data.task.pipeline.server.service;

import com.data.task.pipeline.core.beans.*;
import com.data.task.pipeline.server.beans.TaskPipelineServerOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.*;
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
        //进行master抢夺，若失败进行master监听等待接手
        boolean beMaster = registerMaster();

        //初始化基本行为定义
        if(beMaster){
            serverActionDefinition();
        } else {
            watchMaster();
        }

    }

    private boolean registerMaster() throws Exception {
        String nodeName = TaskPipelineUtils.getLocalNodeName();
        boolean beMaster = false;
        //避免由于异常没有注册成功，导致没有master节点
        while (!operation.checkMasterExist()) {
            try {
                operation.registerMasterNode(nodeName);
                beMaster = true;
            } catch (Exception e) {
                log.info("registerMasterNode failed,this node will be standby,waiting to be master");
            }
            //出现异常每5秒重试注册master
            if(!beMaster && !operation.checkMasterExist()){
                sleep(5000);
            }
        }

        return beMaster;

    }

    private void watchMaster() throws Exception {
        operation.watchMaster(new TaskPipelineMasterStatusListener() {
            @Override
            public void onMasterStatusDelete() {
                log.info("master disappear,going gerneric task pipeline action");
                try {
                    gernericTaskPipelineAction();
                } catch (Exception e) {
                    log.error("gerneric task pipeline action exception:{}",e);
                }
            }
        });
    }


    private void serverActionDefinition() throws Exception {
        //监听task app目录下的新节点变化
        operation.initTaskWatcher(new TaskPipelineAppTaskListener() {
            @Override
            public void onAppTaskSubmit(String appName, String taskName) {
                watchTaskStatusAndAction(appName,taskName);
                assignTask(appName,taskName);
            }
        });
        workerChangeActionDefinition();
        existingTaskActionDefinition();
        existingAssignTaskActionDefinition();
    }

    private void workerChangeActionDefinition() throws Exception {
        List<String> apps = operation.getWorkerAppList();
        apps.forEach(app ->{
            try {
                workerChangeActionDefinition(app);
            } catch (Exception e) {
                log.error("app:{} worker change action definition exception:{}",app,e);
            }
        });
    }

    private void workerChangeActionDefinition(String app) throws Exception {
        operation.watchWorkerList(app, new TaskPipelineWorkerListener(app) {
            @Override
            public void onWorkerDelete(String appName, String node) {
                try {
                    onWorkerDeleteAction(appName,node);
                } catch (Exception e) {
                    log.error("app:{} on worker delete action definition exception:{}",app);
                }

            }
        });
    }

    private void onWorkerDeleteAction(String appName, String worker) throws Exception {
        operation.getAssignTaskList(appName,worker).forEach(assignTask -> {
            try {
                existingAssignTaskActionDefinition(appName, assignTask, Collections.emptyList());
            } catch (Exception e) {
                log.error("app:{} on worker:{} delete action definition exception:{}",appName,worker);
            }
        });
    }


    /**
     * 处理task app目录下的已存在的任务
     * @throws Exception
     */
    private void existingTaskActionDefinition() throws Exception {
        List<String> appList = operation.getTaskAppList();
        appList.forEach(app -> {
            try {
                existingTaskActionDefinition(app);
            } catch (Exception e) {
                log.error("existing app:{} action init exception:{}",app,e);
            }
        });
    }

    private void existingTaskActionDefinition(String app) throws Exception {
        List<String> taskList = operation.getTaskList(app);
        List<String> nodes = operation.getAppNodeList(app);
        taskList.forEach(task -> {
            try {
                existingTaskActionDefinition(app,task,nodes);
            } catch (Exception e) {
                log.error("existing app:{} task:{} action init exception:{}",app,task,e);
            }
        });
    }

    private void existingTaskActionDefinition(String app,String task,List<String> appNodes) throws Exception {
        //如果提交任务的app节点已经消失,标识为missapp并归档
        if(!appNodes.contains(operation.getTaskSubmitAppNode(task))){
            operation.updateTaskStatus(app,task, TaskPipelineCoreConstant.TaskStatus.MISSAPP.status());
            operation.archiveTask(app,task);
            return;
        }

        String status = operation.getTaskStatus(app,task);
        //如果是已经消费或者失去app连接的任务则进行归档
        judgeAndArchiveTask(app,task,status);

        if(!TaskPipelineCoreConstant.TaskStatus.SUBMIT.status().equals(status)){
            return;
        }

        assignTask(app,task);
    }

    /**
     * 处理assign app目录下已经存在的作业
     * @throws Exception
     */
    private void existingAssignTaskActionDefinition() throws Exception {
        List<String> appList = operation.getAssignTaskAppList();
        appList.forEach(app -> {
            try {
                existingAssignTaskActionDefinition(app);
            } catch (Exception e) {
                log.error("existing assign app:{} action init exception:{}",app,e);
            }
        });
    }

    private void existingAssignTaskActionDefinition(String app) throws Exception {
        List<String> assignTaskList= operation.getAssignTaskList(app);
        List<String> nodes = operation.getWorkerList(app);
        assignTaskList.forEach(assignTask -> {
            try {
                existingAssignTaskActionDefinition(app,assignTask,nodes);
            } catch (Exception e) {
                log.error("existing assign app:{} assignTask:{} action init exception:{}",app,assignTask,e);
            }
        });
    }

    private void existingAssignTaskActionDefinition(String app,String assignTask,List<String> workerNodes) throws Exception {
        String status = operation.getAssignTaskStatus(app,assignTask);
        Map<String,String> assignInfoMap = operation.getAssignTaskWorkerInfo(assignTask);
        //归档进行中的已经完成或app节点已经消失的作业
        if(TaskPipelineCoreConstant.TaskStatus.DONE.status().equals(status) ||
                !operation.checkAppNodeExist(app,assignInfoMap.get(APP_Node_NAME))){
            archiveAssignTask(app,assignTask);
            return;
        }
        //若workerNodes为空列表则跳过判断node是否存在的验证
        if(workerNodes.contains(assignInfoMap.get(WORKER))){
            return;
        }
        //处理作业的worker节点已经消失，则重新更新任务状态为resubmit并归档作业
        operation.updateTaskStatus(app,assignInfoMap.get(TASK), TaskPipelineCoreConstant.TaskStatus.RESUBMIT.status());
        archiveAssignTask(app,assignTask);
    }

    private void watchTaskStatusAndAction(String appName, String taskName){
        try {
            operation.watchTaskStatus(appName, taskName, new TaskPipelineTaskStatusListener(appName) {
                @Override
                public void onTaskStatusChange(String appName, String taskName, String status) {
                    //若状态为resubmit则重新分配作业
                    if(TaskPipelineCoreConstant.TaskStatus.RESUBMIT.status().equals(status)) {
                        assignTask(appName,taskName);
                        return;
                    }
                    judgeAndArchiveTask(appName,taskName,status);
                }
            });
        } catch (Exception e){
            log.warn("watchTaskStatus appName:{} taskName:{} exception:{}",appName,taskName,e);
        }
    }

    private void judgeAndArchiveTask(String appName, String taskName,String status){
        if(!TaskPipelineCoreConstant.TaskStatus.CONSUMED.status().equals(status) && !TaskPipelineCoreConstant.TaskStatus.MISSAPP.status().equals(status)) {
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

            TaskPipelineAssignTaskStatusListener assignTaskStatusListener = new TaskPipelineAssignTaskStatusListener(appName,taskName) {
                @Override
                public void onAssignTaskDone(String appName, String assignTaskName) {
                    archiveAssignTask(appName,assignTaskName);
                }
            };
            operation.assignTaskAndWatchStatus(appName,taskName, worker.get("node"),assignTaskStatusListener);
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
