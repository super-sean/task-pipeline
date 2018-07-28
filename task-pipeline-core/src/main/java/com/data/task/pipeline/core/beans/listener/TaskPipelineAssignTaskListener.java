package com.data.task.pipeline.core.beans.listener;

import com.data.task.pipeline.core.beans.TaskPipelineCoreConstant;
import com.data.task.pipeline.core.beans.operation.TaskPipelineOperation;
import com.sun.deploy.util.StringUtils;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.ASSIGN_TASK_SEP;
import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.HISTORY_DIR;
import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.TASK_SEP;

/**
 * @author xinzai
 * @create 2018-07-24 下午5:13
 **/
public abstract class TaskPipelineAssignTaskListener {
    private static Logger log = LoggerFactory.getLogger(TaskPipelineAssignTaskListener.class);
    private String appName;
    private String nodeName;
    private PathChildrenCacheListener listener;
    private TaskPipelineOperation operation;

    public TaskPipelineAssignTaskListener(String appName,String nodeName) {
        this.appName = appName;
        this.nodeName = nodeName;
        listener = (curatorFramework,pathChildrenCacheEvent) -> onTaskListChange(pathChildrenCacheEvent);
    }

    /**
     * 监听任务列表变化回调处理
     * @param pathChildrenCacheEvent
     * @throws Exception
     */
    private void onTaskListChange(PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
        //只监听新增事件
        if(!pathChildrenCacheEvent.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)){
            return;
        }
        String assignTaskName = pathChildrenCacheEvent.getData().getPath().split("/")[3];
        //过滤history目录变化
        if(HISTORY_DIR.replace("/","").equals(assignTaskName)){
            return;
        }
        String[] assignTaskNameSplitArray = assignTaskName.split(ASSIGN_TASK_SEP);
        String assignNodeName = assignTaskNameSplitArray[1];
        log.info("receive task info ",assignTaskName);
        //判断任务是否分配给当前节点
        if(!nodeName.equals(assignNodeName)) {
            return;
        }

        String[] taskNameArray = new String[assignTaskNameSplitArray.length-3];
        System.arraycopy(assignTaskNameSplitArray, 3, taskNameArray, 0, taskNameArray.length);
        //获取task
        String taskName = StringUtils.join(Arrays.asList(taskNameArray),TASK_SEP);

        //检查task状态
        String status = operation.getTaskStatus(appName,taskName);
        if(!TaskPipelineCoreConstant.TaskStatus.SUBMIT.status().equals(status) && !TaskPipelineCoreConstant.TaskStatus.RESUBMIT.status().equals(status)){
            //更新assignTask状态为重复分配
            operation.updateAssignTaskStatus(appName,assignTaskName, TaskPipelineCoreConstant.TaskStatus.REPEAT.status());
            return;
        }
        //更新assignTask状态为Running
        operation.updateAssignTaskStatus(appName,assignTaskName, TaskPipelineCoreConstant.TaskStatus.RUNNING.status());
        //获取params
        String params = operation.getTaskParams(appName,taskName);
        onAssignTaskChange(appName,taskName,params,nodeName);
        //更新assignTask状态为DONE
        operation.updateAssignTaskStatus(appName,assignTaskName, TaskPipelineCoreConstant.TaskStatus.DONE.status());
    }


    /**
     * 用于worker端实现分配任务列表变化时回调
     * @param appName
     * @param taskName
     * @param node
     */
    public abstract void onAssignTaskChange(String appName,String taskName,String params,String node);

    public PathChildrenCacheListener getListener() {
        return listener;
    }

    public void setOperation(TaskPipelineOperation operation) {
        this.operation = operation;
    }
}
