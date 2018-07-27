package com.data.task.pipeline.app.plugin;

import com.data.task.pipeline.core.beans.TaskPipelineCoreConfig;
import com.data.task.pipeline.core.beans.TaskPipelineCoreConstant;
import com.data.task.pipeline.core.beans.TaskPipelineTaskStatusListener;

/**
 * @author xinzai
 * @create 2018-07-23 下午2:22
 **/
public class AppRunnerTest {
    public static void main(String[] args) throws Exception {
        TaskPipelineCoreConfig config = new TaskPipelineCoreConfig("127.0.0.1:2181",1000,0,5,5,10,0,20);
        TaskPipelineAppSupporter supporter = new TaskPipelineAppSupporter("topn_select",config);
        TaskPipelineTaskStatusListener listener = new TaskPipelineTaskStatusListener(supporter.getAppName()) {
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
        Thread thread = new Thread();
        thread.sleep(1000000);
    }
}
