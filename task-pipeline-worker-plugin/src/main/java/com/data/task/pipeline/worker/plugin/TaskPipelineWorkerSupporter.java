package com.data.task.pipeline.worker.plugin;

import com.data.task.pipeline.core.beans.TaskPipelineAssignTaskListener;
import com.data.task.pipeline.core.beans.TaskPipelineCoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author xinzai
 * @create 2018-07-25 上午11:07
 **/
public class TaskPipelineWorkerSupporter {

    private static Logger log = LoggerFactory.getLogger(TaskPipelineWorkerSupporter.class);

    private String appName;

    private TaskPipelineWorkerOperation operation;

    private String nodeName;

    public TaskPipelineWorkerSupporter(String appName,TaskPipelineCoreConfig config) {
        this.appName = appName;
        operation = new TaskPipelineWorkerOperation(appName,config);
        nodeName = System.currentTimeMillis() + "";
        try {
            InetAddress addr = InetAddress.getLocalHost();
            //获取本机ip
            String ip=addr.getHostAddress().toString();
            //获取本机计算机名称
            String hostName=addr.getHostName().toString();
            nodeName = hostName + "-" + ip;
        } catch (UnknownHostException e) {
            log.warn("get host info exception",e);
        }

        try {
            operation.registerWorkerNode(appName,nodeName);
        } catch (Exception e) {
            log.error("register to task-pipeline platform exception",e);
        }
    }

    public void watchAssignTask(TaskPipelineAssignTaskListener listener) throws Exception {
        operation.watchAssignTaskList(appName,listener);
    }

    public void updateTaskStatus(String taskName,String status) throws Exception {
        operation.updateTaskStatus(appName,taskName,status);
    }

    public void fulfilATask(String taskName,String result) throws Exception {
        operation.fulfilATask(appName,taskName,result);
    }

    public String getAppName() {
        return appName;
    }

    public String getNodeName() {
        return nodeName;
    }

    @PreDestroy
    public void destroy() throws Exception {
        operation.removeAppNode(appName,nodeName);

    }
}


