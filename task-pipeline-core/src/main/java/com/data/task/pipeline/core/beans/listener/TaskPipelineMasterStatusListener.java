package com.data.task.pipeline.core.beans.listener;

import com.data.task.pipeline.core.beans.operation.TaskPipelineOperation;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xinzai
 * create 2018-07-24 下午5:13
 **/
public abstract class TaskPipelineMasterStatusListener {
    private static Logger log = LoggerFactory.getLogger(TaskPipelineMasterStatusListener.class);
    private NodeCache cache;
    private NodeCacheListener listener;
    private TaskPipelineOperation operation;

    public TaskPipelineMasterStatusListener() {
        listener = () -> {
            if(cache.getCurrentData() != null){
                return;
            }
            onMasterStatusDelete();
        };
    }

    /**
     * 用于master失效时回调
     */
    public abstract void onMasterStatusDelete();

    public NodeCache getCache() {
        return cache;
    }

    public void setCache(NodeCache cache) {
        this.cache = cache;
    }

    public NodeCacheListener getListener() {
        return listener;
    }

    public void setOperation(TaskPipelineOperation operation) {
        this.operation = operation;
    }

}
