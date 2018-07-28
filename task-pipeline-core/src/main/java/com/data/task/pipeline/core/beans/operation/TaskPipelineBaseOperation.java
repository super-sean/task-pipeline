package com.data.task.pipeline.core.beans.operation;

import com.data.task.pipeline.core.beans.config.TaskPipelineCoreConfig;
import com.data.task.pipeline.core.beans.TaskPipelineCuratorFrameworkFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.concurrent.*;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.NAMESPACE;

/**
 * @author xinzai
 * @create 2018-07-23 下午4:08
 **/
public abstract class TaskPipelineBaseOperation {

    private CuratorFramework cf;

    private ExecutorService pool;

    public TaskPipelineBaseOperation(TaskPipelineCoreConfig config) {
        TaskPipelineCuratorFrameworkFactory taskPipelineCuratorFrameworkFactory = new TaskPipelineCuratorFrameworkFactory(NAMESPACE, config.getZkConnectStr(), config.getSessionTimeout(), config.getBaseSleepTimeMs(), config.getMaxRetries());
        this.cf = taskPipelineCuratorFrameworkFactory.getCuratorFramework();
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("task-pipeline-callback-pool-%d").build();
        pool = new ThreadPoolExecutor(config.getCorePoolSize(), config.getMaxthreadPoolSize(), config.getKeepApiveTime(), TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(config.getQueueSize()), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        cf.start();
    }

    public void createNode(String nodePath,String value,CreateMode... mode) throws Exception {
        CreateMode createMode = CreateMode.PERSISTENT;
        if(mode.length > 0) {
            createMode = mode[0];
        }
        cf.create()
                .creatingParentsIfNeeded()
                .withMode(createMode)
                .forPath(nodePath, value.getBytes());
    }

    public String getNodeValue(String nodePath) throws Exception {
        return new String(cf.getData().forPath(nodePath));
    }

    public List<String> getNodeChildren(String nodePath) throws Exception {
        return cf.getChildren().forPath(nodePath);
    }


    public void updateNodeValue(String nodePath,String value) throws Exception {
        cf.setData().forPath(nodePath, value.getBytes());
    }

    public void deleteNode(String nodePath) throws Exception {
        cf.delete().guaranteed().deletingChildrenIfNeeded().forPath(nodePath);
    }

    public boolean checkNodeExist(String nodePath) throws Exception {
        return cf.checkExists().forPath(nodePath) != null;
    }

    public NodeCache watchNode(String nodePath,NodeCacheListener listener) throws Exception {
        NodeCache cache = new NodeCache(cf, nodePath, false);
        cache.start(true);
        cache.getListenable().addListener(listener,pool);
        return cache;
    }

    public void watchChildrenNodes(String nodePath,PathChildrenCacheListener listener) throws Exception {
        PathChildrenCache cache = new PathChildrenCache(cf, nodePath, false);
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        cache.getListenable().addListener(listener);
    }

    @PreDestroy
    public void destory(){
        pool.shutdown();
        cf.close();
    }
    
}
