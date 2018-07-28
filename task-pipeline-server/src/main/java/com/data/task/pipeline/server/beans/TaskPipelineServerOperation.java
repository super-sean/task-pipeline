package com.data.task.pipeline.server.beans;

import com.data.task.pipeline.core.beans.*;
import com.data.task.pipeline.core.beans.listener.*;
import com.data.task.pipeline.core.beans.operation.TaskPipelineOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.*;
import static java.lang.Thread.sleep;

/**
 * @author xinzai
 * @create 2018-07-25 下午4:36
 **/
public class TaskPipelineServerOperation extends TaskPipelineOperation {

    private static Logger log = LoggerFactory.getLogger(TaskPipelineServerOperation.class);

    public TaskPipelineServerOperation(TaskPipelineCoreConfig config) {
        super(config);
    }

    /**
     * 初始化系统目录
     * @throws Exception
     */
    public void initNamespacePath() throws Exception {
        checkPathAndCreate(APPS_PATH.substring(0,APPS_PATH.length() - 1));
        checkPathAndCreate(WORKERS_PATH.substring(0,WORKERS_PATH.length() - 1));
        checkPathAndCreate(TASKS_PATH.substring(0,TASKS_PATH.length() - 1));
        checkPathAndCreate(ASSIGN_PATH.substring(0,ASSIGN_PATH.length() - 1));

    }

    private void checkPathAndCreate(String path) throws Exception {
        if(!checkNodeExist(path)){
            createNode(path,"");
        }
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

        //监听task app新目录下的节点变化
        watchTaskAppList(new TaskPipelineFunctionAppListListener() {
            @Override
            public void onAppAdd(String appName) {
                try {
                    watchTaskList(appName, listener);
                } catch (Exception e) {
                    log.error("watch new app:{} task list exception:{}",appName,e);
                }
            }
        });

    }


    public void assignTaskAndWatchStatus(String appName, String taskName, String worker,TaskPipelineAssignTaskStatusListener assignTaskStatusListener) throws Exception {
        assignTask(appName,taskName, worker);
        watchAssignTaskStatus(appName,taskName,worker,assignTaskStatusListener);
    }

    public void noWorkerAssignResponse(String appName, String taskName) throws Exception {
        updateTaskStatus(appName,taskName, TaskPipelineCoreConstant.TaskStatus.NOWORKER.status());
    }

    public Map<String,String> getWorker(String appName) throws Exception {
        List<String> workers = getWorkerList(appName);
        //间隔10秒重新获取一次
        if(workers.size() == 0){
            sleep(10000);
            workers = getWorkerList(appName);
        }
        final int[] weight = {Integer.MAX_VALUE};
        final String[] workerNode = {""};
        Map<String,String> worker = new WeakHashMap<>();
        //获取权重最小的第一个节点作为worker
        workers.forEach(w -> {
            try {
                int tmpWeight = getWorkerWeight(appName,w);
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

    public void assignTask(String appName, String taskName){
        Map<String,String> worker = new WeakHashMap<>();
        try {
            worker = getWorker(appName);
            if(StringUtils.isEmpty(worker.get("node"))){
                log.warn("no worker for app:{} task:{}",appName,taskName);
                noWorkerAssignResponse(appName,taskName);
                return;
            }

            TaskPipelineAssignTaskStatusListener assignTaskStatusListener = new TaskPipelineAssignTaskStatusListener(appName,taskName) {
                @Override
                public void onAssignTaskDone(String appName, String assignTaskName) {
                    try {
                        archiveAssignTask(appName,assignTaskName);
                    } catch (Exception e) {
                        log.warn("archive app:{} assign Task:{} exception:{}",appName,assignTaskName,e);
                    }
                }
            };
            assignTaskAndWatchStatus(appName,taskName, worker.get("node"),assignTaskStatusListener);
            updateWorkerWeight(appName,worker.get("node"),(Integer.parseInt(worker.get("weight")) + 1) + "");
        } catch (Exception e) {
            log.error("assign worker:{} for app:{} task:{} exception:{}", worker,appName,taskName,e);
        }
    }


    public void judgeAndArchiveTask(String appName, String taskName,String status){
        if(!TaskPipelineCoreConstant.TaskStatus.CONSUMED.status().equals(status) && !TaskPipelineCoreConstant.TaskStatus.MISSAPP.status().equals(status)) {
            return;
        }

        try {
            archiveTask(appName,taskName);
        } catch (Exception e) {
            log.error("archiveTask appName:{} taskName:{} exception:{}",appName,taskName,e);
        }
    }

    public void watchTaskStatusAndAction(String appName, String taskName){
        try {
            watchTaskStatus(appName, taskName, new TaskPipelineTaskStatusListener(appName) {
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

    public void existingAssignTaskActionDefinition(String app) throws Exception {
        List<String> assignTaskList= getAssignTaskList(app);
        List<String> nodes = getWorkerList(app);
        assignTaskList.forEach(assignTask -> existingAssignTaskActionDefinition(app,assignTask,nodes));
    }

    public void existingAssignTaskActionDefinition(String app,String assignTask,List<String> workerNodes){
        try {
            String status = getAssignTaskStatus(app,assignTask);
            Map<String,String> assignInfoMap = getAssignTaskWorkerInfo(assignTask);
            //归档进行中的已经完成或app节点已经消失的作业
            if(TaskPipelineCoreConstant.TaskStatus.DONE.status().equals(status) ||
                    !checkAppNodeExist(app,assignInfoMap.get(APP_Node_NAME))){
                archiveAssignTask(app,assignTask);
                return;
            }
            //若workerNodes为空列表则跳过判断node是否存在的验证
            if(workerNodes.contains(assignInfoMap.get(WORKER))){
                return;
            }
            //处理作业的worker节点已经消失，则重新更新任务状态为resubmit并归档作业
            updateTaskStatus(app,assignInfoMap.get(TASK), TaskPipelineCoreConstant.TaskStatus.RESUBMIT.status());
            archiveAssignTaskToWorker(app,assignTask);
        } catch (Exception e) {
            log.error("existing app:{} assigntask:{} workers:{} action definition exception:{}",app,assignTask,workerNodes);
        }
    }

    public void archiveAssignTaskToWorker(String appName, String assignTaskName){
        try {
            archiveAssignTask(appName,assignTaskName);
        } catch (Exception e) {
            log.warn("archive app:{} assign Task:{} exception:{}",appName,assignTaskName,e);
        }
    }



    public void existingTaskActionDefinition(String app) throws Exception {
        List<String> taskList = getTaskList(app);
        List<String> nodes = getAppNodeList(app);
        taskList.forEach(task ->existingTaskActionDefinition(app,task,nodes));
    }

    private void existingTaskActionDefinition(String app,String task,List<String> appNodes){
        try {
            //如果提交任务的app节点已经消失,标识为missapp并归档
            if(!appNodes.contains(getTaskSubmitAppNode(task))){
                updateTaskStatus(app,task, TaskPipelineCoreConstant.TaskStatus.MISSAPP.status());
                archiveTask(app,task);
                return;
            }

            String status = getTaskStatus(app,task);
            //如果是已经消费或者失去app连接的任务则进行归档
            judgeAndArchiveTask(app,task,status);

            if(!TaskPipelineCoreConstant.TaskStatus.SUBMIT.status().equals(status)){
                return;
            }

            assignTask(app,task);
        } catch (Exception e) {
            log.error("existing app:{} task:{} action init exception:{}",app,task,e);
        }
    }


    public void workerChangeActionDefinition(String app) throws Exception {
        watchWorkerList(app, new TaskPipelineWorkerListener(app) {
            @Override
            public void onWorkerDelete(String appName, String node) {
                    onWorkerDeleteAction(appName,node);
            }
        });
    }

    private void onWorkerDeleteAction(String appName, String worker) {
        try {
            getAssignTaskList(appName,worker)
                    .forEach(assignTask -> existingAssignTaskActionDefinition(appName, assignTask, Collections.emptyList()));
        } catch (Exception e) {
            log.error("app:{} on worker delete action definition exception:{}",appName);
        }
    }


    public void watchMasterChange(MasterChangeAction action) throws Exception {
        watchMaster(new TaskPipelineMasterStatusListener() {
            @Override
            public void onMasterStatusDelete() {
                log.info("master disappear,going gerneric task pipeline action");
                try {
                    action.onMasterChange();
                } catch (Exception e) {
                    log.error("gerneric task pipeline action exception:{}",e);
                }
            }
        });
    }

    public boolean registerMaster() throws Exception {
        String nodeName = TaskPipelineUtils.getLocalNodeName();
        boolean beMaster = false;
        //避免由于异常没有注册成功，导致没有master节点
        while (!checkMasterExist()) {
            try {
                registerMasterNode(nodeName);
                beMaster = true;
            } catch (Exception e) {
                log.info("registerMasterNode failed,this node will be standby,waiting to be master");
            }
            //出现异常每5秒重试注册master
            if(!beMaster && !checkMasterExist()){
                sleep(5000);
            }
        }

        return beMaster;

    }


}
