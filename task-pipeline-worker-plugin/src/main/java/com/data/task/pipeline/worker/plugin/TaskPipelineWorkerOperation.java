package com.data.task.pipeline.worker.plugin;

import com.data.task.pipeline.core.beans.config.TaskPipelineCoreConfig;
import com.data.task.pipeline.core.beans.operation.TaskPipelineOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.APPS_PATH;
import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.ASSIGN_PATH;
import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.WORKERS_PATH;

/**
 * @author xinzai
 * create 2018-07-25 上午10:31
 **/
public class TaskPipelineWorkerOperation extends TaskPipelineOperation {

    private static Logger log = LoggerFactory.getLogger(TaskPipelineWorkerOperation.class);

    private String appName;

    public TaskPipelineWorkerOperation(String appName,TaskPipelineCoreConfig config) {
        super(config);
        this.appName = appName;
    }

    public void registerWorker(String node) throws Exception {
        registerWorkerNode(appName,node);
        if (!checkNodeExist(ASSIGN_PATH + appName)) {
            createNode(ASSIGN_PATH + appName,"");
        }
    }

    /**
     * 尝试重新注册
     */
    public void retryRegisterWorker(String node){
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 参数：1、任务体 2、首次执行的延时时间 3、任务执行间隔 4、间隔时间单位
        service.scheduleAtFixedRate(()->{
            try {
                if(!checkNodeExist(WORKERS_PATH + appName + "/" + node)) {
                    log.info("retry to registerWorker,appName={},nodeName={}",new Object[]{appName,node});
                    registerWorker(node);
                    log.info("retry to registerWorker success");
                }
            } catch (Exception e) {
                log.error("retry to registerWorker failed");
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

}
