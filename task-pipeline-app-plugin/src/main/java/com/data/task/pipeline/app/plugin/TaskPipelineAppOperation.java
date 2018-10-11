package com.data.task.pipeline.app.plugin;

import com.data.task.pipeline.core.beans.config.TaskPipelineCoreConfig;
import com.data.task.pipeline.core.beans.operation.TaskPipelineOperation;
import com.data.task.pipeline.core.beans.listener.TaskPipelineTaskStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.APPS_PATH;
import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.TASKS_PATH;

/**
 * @author xinzai
 * create 2018-07-24 下午3:27
 **/
public class TaskPipelineAppOperation extends TaskPipelineOperation {

    private static Logger log = LoggerFactory.getLogger(TaskPipelineAppOperation.class);

    private String appName;

    public TaskPipelineAppOperation(String appName,TaskPipelineCoreConfig config) {
        super(config);
        this.appName = appName;
    }

    public void registerApp(String node) throws Exception {
        registerAppNode(appName,node);
        if (!checkNodeExist(TASKS_PATH + appName)) {
             createNode(TASKS_PATH + appName,"");
        }
    }

    public void submitTask(String taskName,String params,TaskPipelineTaskStatusListener listener) throws Exception {
        submitTaskNode(appName,taskName,params);
        watchTaskStatus(appName,taskName,listener);
    }

    /**
     * 尝试重新注册
     */
    public void retryRegisterApp(String node){
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        // 参数：1、任务体 2、首次执行的延时时间 3、任务执行间隔 4、间隔时间单位
        service.scheduleAtFixedRate(()->{
            try {
                if(!checkNodeExist(APPS_PATH + appName + "/" + node)) {
                    log.info("retry to registerApp,appName={},nodeName={}",new Object[]{appName,node});
                    registerApp(node);
                    log.info("retry to registerApp success");
                }
            } catch (Exception e) {
                log.error("retry to registerApp failed");
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

}
