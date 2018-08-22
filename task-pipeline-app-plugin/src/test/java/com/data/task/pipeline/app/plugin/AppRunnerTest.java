package com.data.task.pipeline.app.plugin;

import com.data.task.pipeline.core.beans.TaskPipelineCoreConstant;
import com.data.task.pipeline.core.beans.config.TaskPipelineCoreConfig;
import com.data.task.pipeline.core.beans.listener.TaskPipelineTaskStatusListener;

import java.util.ArrayList;
import java.util.List;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.APP;

/**
 * @author xinzai
 * create 2018-07-23 下午2:22
 **/
public class AppRunnerTest {
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
        aclIds.add("tp_app:^zskxtpapp123456");
        aclIds.add("tp_server:^zskxtpserver123456");
        config.setAclIds(aclIds);
        config.setAclId("tp_app:^zskxtpapp123456");
        TaskPipelineAppSupporter supporter = new TaskPipelineAppSupporter("test1",config);
        TaskPipelineTaskStatusListener listener = new TaskPipelineTaskStatusListener(APP,supporter.getAppName()) {
            @Override
            public void onTaskStatusChange(String appName, String taskName, String status) {
                System.out.println(appName);
                System.out.println(taskName);
                System.out.println(status);
                if(TaskPipelineCoreConstant.TaskStatus.DONE.status().equals(status)){
                   System.out.println(getTaskResult());
                }
            }
        };

        supporter.submitTask("{\"par1\":\"test\"}",listener);
        System.out.println("---------init task :" + supporter.getTaskStatus(listener.getTaskName()));
        System.out.println("--------- sync result:" + listener.getSyncTaskResult());
        Thread thread = new Thread();
        thread.sleep(1000000);
    }
}
