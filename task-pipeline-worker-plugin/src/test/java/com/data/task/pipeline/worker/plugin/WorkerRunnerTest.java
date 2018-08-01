package com.data.task.pipeline.worker.plugin;

import com.data.task.pipeline.core.beans.listener.TaskPipelineAssignTaskListener;
import com.data.task.pipeline.core.beans.config.TaskPipelineCoreConfig;
import com.data.task.pipeline.core.beans.TaskPipelineCoreConstant;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Thread.sleep;

/**
 * @author xinzai
 * @create 2018-07-25 上午11:13
 **/
public class WorkerRunnerTest {

    public static void main(String[] args) throws Exception {
        TaskPipelineCoreConfig config = new TaskPipelineCoreConfig();
        config.setZkConnectStr("127.0.0.1:2181");
        config.setSessionTimeout(1000);
        config.setBaseSleepTimeMs(0);
        config.setMaxRetries(5);
        config.setCorePoolSize(5);
        config.setMaxthreadPoolSize(10);
        config.setKeepApiveTime(0);
        config.setQueueSize(20);
        List<String> aclIds = new ArrayList<>();
        aclIds.add("tp_worker:^zskxtpworker123456");
        aclIds.add("tp_server:^zskxtpserver123456");
        config.setAclIds(aclIds);
        config.setAclId("tp_worker:^zskxtpworker123456");
        TaskPipelineWorkerSupporter supporter = new TaskPipelineWorkerSupporter("test1",config);
        TaskPipelineAssignTaskListener listener = new TaskPipelineAssignTaskListener(supporter.getAppName(),supporter.getNodeName()) {
            @Override
            public void onAssignTaskChange(String appName, String taskName, String params,String node) {
                System.out.println(appName);
                System.out.println(params);
                System.out.println(taskName);
                System.out.println(node);
                try {
                    supporter.updateTaskStatus(taskName, TaskPipelineCoreConstant.TaskStatus.RUNNING.status());
                    sleep(10000);
                    supporter.fulfilATask(taskName,"{\"type\":\"content\",\"value\":\"something\"}");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        };
        supporter.watchAssignTask(listener);
        sleep(1000000);
    }
}
