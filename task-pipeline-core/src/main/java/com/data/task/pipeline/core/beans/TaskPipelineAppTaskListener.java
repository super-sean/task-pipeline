package com.data.task.pipeline.core.beans;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.HISTORY_DIR;

/**
 * @author xinzai
 * @create 2018-07-25 下午5:21
 **/
public abstract class TaskPipelineAppTaskListener {
    private static Logger log = LoggerFactory.getLogger(TaskPipelineAppTaskListener.class);
    private PathChildrenCacheListener listener;
    private TaskPipelineOperation operation;

    public TaskPipelineAppTaskListener() {
        listener = (curatorFramework,pathChildrenCacheEvent) -> onTaskListAdd(pathChildrenCacheEvent);
    }

    private void onTaskListAdd(PathChildrenCacheEvent event){
        //只监听新增事件
        if(!event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)){
            return;
        }
        String[] pathArray = event.getData().getPath().split("/");
        String appName = pathArray[2];
        String taskName = pathArray[3];
        //过滤history目录变化
        if(HISTORY_DIR.replace("/","").equals(taskName)){
            return;
        }
        onAppTaskSubmit(appName,taskName);
    }
    /**
     * 用于server端实现获取任务列表变化时回调
     * @param appName
     * @param taskName
     */
    public abstract void onAppTaskSubmit(String appName,String taskName);

    protected PathChildrenCacheListener getListener(){
        return listener;
    }

    protected void setOperation(TaskPipelineOperation operation){
        this.operation = operation;
    }

    public TaskPipelineOperation getOperation() {
        return operation;
    }
}
