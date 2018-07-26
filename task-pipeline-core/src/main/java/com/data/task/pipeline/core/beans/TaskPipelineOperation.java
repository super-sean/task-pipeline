package com.data.task.pipeline.core.beans;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
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
        createNode(APPS_PATH + appName + "/" + node,"");
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
        createNode(WORKERS_PATH + appName + "/" + node,WORKER_INIT_WEIGHT);
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
        createNode(ASSIGN_PATH + appName + "/worker" + ASSIGN_TASK_SEP + worker + ASSIGN_TASK_SEP + "task" + ASSIGN_TASK_SEP + taskName,"");
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
        NodeCache nodeCache = watchNode(TASKS_PATH + appName + "/" + taskName + TASKS_STATUS, listener.getListener());
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

    public List<String> getTaskAppList() throws Exception {
        return getNodeChildren(TASKS_PATH.substring(0,TASKS_PATH.length() - 1));
    }

    public List<String> getWorkerList(String appName) throws Exception {
        return getNodeChildren(WORKERS_PATH + appName);
    }
}
