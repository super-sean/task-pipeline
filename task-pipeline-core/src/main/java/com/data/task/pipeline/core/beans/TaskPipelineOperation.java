package com.data.task.pipeline.core.beans;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.*;

/**
 * @author xinzai
 * @create 2018-07-23 下午5:57
 **/
public abstract class TaskPipelineOperation extends TaskPipelineBaseOperation {

    private static Logger log = LoggerFactory.getLogger(TaskPipelineOperation.class);

    public TaskPipelineOperation(TaskPipelineCoreConfig config) {
        super(config);
    }

    /**
     * 注册app节点
     * @param appName
     * @param node
     * @throws Exception
     */
    public void registerAppNode(String appName,String node) throws Exception {
        if(checkNodeExist(APPS_PATH + appName + "/" + node)) {
            return;
        }
        createNode(APPS_PATH + appName + "/" + node,"",CreateMode.EPHEMERAL);
    }

    /**
     * 注册worker节点
     * @param appName
     * @param node
     * @throws Exception
     */
    public void registerWorkerNode(String appName,String node) throws Exception {
        if(checkNodeExist(WORKERS_PATH + appName + "/" + node)) {
            return;
        }
        //初始化worker权重为0
        createNode(WORKERS_PATH + appName + "/" + node,WORKER_INIT_WEIGHT, CreateMode.EPHEMERAL);
    }

    public Integer getWorkerWeight(String appName,String node) throws Exception {
        String nodePath = WORKERS_PATH + appName + "/" + node;
        if(!checkNodeExist(nodePath)) {
            return Integer.MAX_VALUE;
        }
        return Integer.parseInt(getNodeValue(nodePath));
    }

    public void updateWorkerWeight(String appName,String node,String weight) throws Exception {
        updateNodeValue(WORKERS_PATH + appName + "/" + node,weight);
    }

    /**
     * 移除app节点
     * @param appName
     * @param node
     * @throws Exception
     */
    public void removeAppNode(String appName,String node) throws Exception {
        deleteNode(APPS_PATH + appName + node);
    }

    /**
     * 移除worker节点
     * @param appName
     * @param node
     * @throws Exception
     */
    public void removeWorkerNode(String appName,String node) throws Exception {
        deleteNode(WORKERS_PATH + appName + node);
    }

    /**
     * 创建任务节点
     * @param appName
     * @param taskName
     * @param params
     * @throws Exception
     */
    public void submitTaskNode(String appName,String taskName,String params) throws Exception {
        createNode(TASKS_PATH + appName + "/" + taskName,"");
        createNode(TASKS_PATH + appName + "/" + taskName + TASKS_PARAMS,params);
        createNode(TASKS_PATH + appName + "/" + taskName + TASKS_STATUS,TaskStatus.SUBMIT.status());
    }

    /**
     * 获取任务状态
     * @param appName
     * @param taskName
     * @return
     * @throws Exception
     */
    public String getTaskStatus(String appName,String taskName) throws Exception {
        return getNodeValue(TASKS_PATH + appName + "/" + taskName + TASKS_STATUS);
    }

    /**
     * 获取任务状态
     * @param appName
     * @param taskName
     * @return
     * @throws Exception
     */
    public String getTaskParams(String appName,String taskName) throws Exception {
        return getNodeValue(TASKS_PATH + appName + "/" + taskName + TASKS_PARAMS);
    }

    /**
     * 获取任务结果
     * @param appName
     * @param taskName
     * @return
     * @throws Exception
     */
    public String getTaskResult(String appName,String taskName) throws Exception {
        return getNodeValue(TASKS_PATH + appName + "/" + taskName + TASKS_RESULT);
    }

    /**
     * 分配任务
     * @param appName
     * @param taskName
     * @param worker
     * @throws Exception
     */
    public void assignTask(String appName,String taskName,String worker) throws Exception {
        createNode(ASSIGN_PATH + appName + "/worker" + ASSIGN_TASK_SEP + worker + ASSIGN_TASK_SEP + "task" + ASSIGN_TASK_SEP + taskName,TaskStatus.SUBMIT.status());
    }

    /**
     * 更新任务状态
     * @param appName
     * @param taskName
     * @param status
     * @throws Exception
     */
    public void updateTaskStatus(String appName,String taskName,String status) throws Exception {
        updateNodeValue(TASKS_PATH + appName + "/" + taskName + TASKS_STATUS,status);
    }

    /**
     * 更新作业状态
     * @param appName
     * @param assigntaskName
     * @param status
     * @throws Exception
     */
    public void updateAssignTaskStatus(String appName,String assigntaskName,String status) throws Exception {
        updateNodeValue(ASSIGN_PATH + appName + "/" + assigntaskName,status);
    }

    /**
     * 完成任务
     * @param appName
     * @param taskName
     * @param result
     * @throws Exception
     */
    public void fulfilATask(String appName,String taskName,String result) throws Exception {
        String nodePath = TASKS_PATH + appName + "/" + taskName + TASKS_RESULT;
        if(checkNodeExist(nodePath)){
            updateNodeValue(nodePath,result);
        } else {
            createNode(nodePath,result);
        }
        updateNodeValue(TASKS_PATH + appName + "/" + taskName + TASKS_STATUS,TaskStatus.DONE.status());
    }

    /**
     * 监听任务状态变化
     * @param appName
     * @param taskName
     * @param listener
     * @throws Exception
     */
    public void watchTaskStatus(String appName,String taskName,TaskPipelineTaskStatusListener listener) throws Exception {
        listener.setOperation(this);
        listener.setTaskName(taskName);
        NodeCache nodeCache = watchNode(TASKS_PATH + appName + "/" + taskName + TASKS_STATUS, listener.getListener());
        listener.setCache(nodeCache);
    }

    /**
     * 监听作业状态变化
     * @param appName
     * @param taskName
     * @param worker
     * @param listener
     * @throws Exception
     */
    public void watchAssignTaskStatus(String appName,String taskName,String worker,TaskPipelineAssignTaskStatusListener listener) throws Exception {
        String assignTaskName ="worker" + ASSIGN_TASK_SEP + worker + ASSIGN_TASK_SEP + "task" + ASSIGN_TASK_SEP + taskName;
        String assignTaskPath = ASSIGN_PATH + appName + "/" + assignTaskName;
        listener.setOperation(this);
        listener.setAssignTaskName(assignTaskName);
        NodeCache nodeCache = watchNode(assignTaskPath, listener.getListener());
        listener.setCache(nodeCache);
    }

    public boolean checkTaskResultExist(String appName,String taskName) throws Exception {
        return checkNodeExist(TASKS_PATH + appName + "/" + taskName + TASKS_RESULT);
    }

    /**
     * 监听业务列表变化
     * @param listener
     * @throws Exception
     */
    public void watchTaskAppList(PathChildrenCacheListener listener) throws Exception {
        watchChildrenNodes(TASKS_PATH,listener);
    }

    /**
     * 监听app任务列表变化
     * @param appName
     * @param listener
     * @throws Exception
     */
    public void watchTaskList(String appName,TaskPipelineAppTaskListener listener) throws Exception {
        listener.setOperation(this);
        watchChildrenNodes(TASKS_PATH + appName,listener.getListener());
    }

    /**
     * 监听assign任务列表变化
     * @param appName
     * @param listener
     * @throws Exception
     */
    public void watchAssignTaskList(String appName,TaskPipelineAssignTaskListener listener) throws Exception {
        listener.setOperation(this);
        watchChildrenNodes(ASSIGN_PATH + appName,listener.getListener());
    }

    /**
     * 获取任务app列表
     * @return
     * @throws Exception
     */
    public List<String> getTaskAppList() throws Exception {
        return getNodeChildren(TASKS_PATH.substring(0,TASKS_PATH.length() - 1));
    }

    /**
     * 获取对应app的worker
     * @param appName
     * @return
     * @throws Exception
     */
    public List<String> getWorkerList(String appName) throws Exception {
        return getNodeChildren(WORKERS_PATH + appName);
    }

    /**
     * 归档Task
     * @param appName
     * @param taskName
     * @throws Exception
     */
    public void archiveTask(String appName,String taskName) throws Exception {
        String taskPath = TASKS_PATH + appName + "/" + taskName;
        String paramsPath = TASKS_PATH + appName + "/" + taskName + TASKS_PARAMS;
        String stausPath = TASKS_PATH + appName + "/" + taskName + TASKS_STATUS;
        String resultPath = TASKS_PATH + appName + "/" + taskName + TASKS_RESULT;
        createNode(TASKS_PATH + appName + HISTORY_DIR + taskName + TASKS_PARAMS,getNodeValue(paramsPath));
        createNode(TASKS_PATH + appName + HISTORY_DIR + taskName + TASKS_STATUS,getNodeValue(stausPath));
        createNode(TASKS_PATH + appName + HISTORY_DIR + taskName + TASKS_RESULT,getNodeValue(resultPath));
        deleteNode(taskPath);
    }

    /**
     * 归档作业
     * @param appName
     * @param assignTaskName
     * @throws Exception
     */
    public void archiveAssignTask(String appName,String assignTaskName) throws Exception {
        String originPath = ASSIGN_PATH + appName + "/" + assignTaskName;
        createNode(ASSIGN_PATH + appName + HISTORY_DIR + assignTaskName,getNodeValue(originPath));
        deleteNode(originPath);
    }
}
