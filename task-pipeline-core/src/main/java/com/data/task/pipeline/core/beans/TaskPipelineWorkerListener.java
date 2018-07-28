package com.data.task.pipeline.core.beans;

import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type.CHILD_REMOVED;

/**
 * @author xinzai
 * @create 2018-07-24 下午5:13
 **/
public abstract class TaskPipelineWorkerListener {
    private static Logger log = LoggerFactory.getLogger(TaskPipelineWorkerListener.class);
    private String appName;
    private String node;
    private NodeCache cache;
    private PathChildrenCacheListener listener;
    private TaskPipelineOperation operation;

    public TaskPipelineWorkerListener(String appName) {
        this.appName = appName;
        listener = (curatorFramework,pathChildrenCacheEvent) -> {
            if(pathChildrenCacheEvent.getData() == null){
                return;
            }
            String node = pathChildrenCacheEvent.getData().getPath().split("/")[3];
            this.node = node;
            log.info("app:{} worker:{} event:{}",appName,node,pathChildrenCacheEvent.getType());
            if(pathChildrenCacheEvent.getType() != CHILD_REMOVED){
                return;
            }
            onWorkerDelete(appName,node);
        };
    }

    /**
     * 监听worker删除操作
     * @param appName
     * @param node
     */
    public abstract void onWorkerDelete(String appName,String node);

    public NodeCache getCache() {
        return cache;
    }

    public void setCache(NodeCache cache) {
        this.cache = cache;
    }

    public PathChildrenCacheListener getListener() {
        return listener;
    }

    protected void setOperation(TaskPipelineOperation operation) {
        this.operation = operation;
    }

}
