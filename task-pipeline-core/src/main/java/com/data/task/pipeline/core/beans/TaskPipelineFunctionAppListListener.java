package com.data.task.pipeline.core.beans;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xinzai
 * @create 2018-07-25 下午5:21
 **/
public abstract class TaskPipelineFunctionAppListListener {
    private static Logger log = LoggerFactory.getLogger(TaskPipelineFunctionAppListListener.class);
    private PathChildrenCacheListener listener;
    private TaskPipelineOperation operation;

    public TaskPipelineFunctionAppListListener() {
        listener = (curatorFramework,pathChildrenCacheEvent) -> onAppListAdd(pathChildrenCacheEvent);
    }

    private void onAppListAdd(PathChildrenCacheEvent event){
        //只监听新增事件
        if(!event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)){
            return;
        }
        log.info("new app:{} ",event.getData().getPath());
        String[] pathArray = event.getData().getPath().split("/");
        String appName = pathArray[2];
        onAppAdd(appName);
    }
    /**
     * 用于server端实现获取任务列表变化时回调
     * @param appName
     */
    public abstract void onAppAdd(String appName);

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
