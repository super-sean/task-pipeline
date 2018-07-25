package com.data.task.pipeline.app.plugin;

import com.data.task.pipeline.core.beans.TaskPipelineCoreConfig;
import com.data.task.pipeline.core.beans.TaskPipelineTaskStatusListener;

/**
 * @author xinzai
 * @create 2018-07-23 下午2:22
 **/
public class RunnerTest {
    public static void main(String[] args) throws Exception {
        TaskPipelineCoreConfig config = new TaskPipelineCoreConfig("192.168.1.89:2181",1000,0,5,5,10,0,20);
        TaskPipelineAppSupporter supporter = new TaskPipelineAppSupporter("topn_select",config);
        String taskName = supporter.getNodeName() + "-" + System.currentTimeMillis();
        TaskPipelineTaskStatusListener listener = new TaskPipelineTaskStatusListener(supporter.getAppName(),taskName) {
            @Override
            public void onTaskStatusChange(String appName, String taskName, String status) {
                System.out.println(appName);
                System.out.println(taskName);
                System.out.println(status);
            }
        };

        supporter.submitTask(taskName,"{\"par1\":\"test\"}",listener);
        Thread thread = new Thread();
        thread.sleep(1000000);
    }
}
