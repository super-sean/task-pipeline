package com.data.task.pipeline.server.service;

import com.data.task.pipeline.core.beans.listener.TaskPipelineAppTaskListener;
import com.data.task.pipeline.core.beans.listener.TaskPipelineFunctionAppListListener;
import com.data.task.pipeline.server.beans.TaskPipelineServerOperation;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * @author xinzai
 * @create 2018-07-25 下午4:58
 **/
@Service
public class TaskPipelineService {
    private static Logger log = LoggerFactory.getLogger(TaskPipelineService.class);

    @Autowired
    private TaskPipelineServerOperation operation;

    @PostConstruct
    public void gernericTaskPipelineAction() throws Exception {
        //进行master抢夺，若失败进行master监听等待接手
        operation.registerMaster(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                log.info("taking the leadership. begin to work.");
                try {
                    serverActionDefinition();
                } catch (Exception e) {
                    log.error("server action definition exception",e);
                }
            }

            @Override
            public void notLeader() {
                log.info("relinquishing the leadership.begin to await.");
            }
        });
    }

    private void serverActionDefinition() throws Exception {
        //初始化目录
        operation.initNamespacePath();
        //监听task app目录下的节点变化
        taskWatcherDefinition();
        //监听worker变化
        workerChangeActionDefinition();
        //处理已经存在的任务
        existingTaskActionDefinition();
        //处理已经存在的作业
        existingAssignTaskActionDefinition();
    }

    /**
     * 监听task app目录下的节点变化
     * @throws Exception
     */
    private void taskWatcherDefinition() throws Exception {
        operation.initTaskWatcher(new TaskPipelineAppTaskListener() {
            @Override
            public void onAppTaskSubmit(String appName, String taskName) {
                operation.watchTaskStatusAndAction(appName,taskName);
                operation.assignTask(appName,taskName);
            }
        });
    }

    /**
     * worker相关变化行为初始化
     * @throws Exception
     */
    private void workerChangeActionDefinition() throws Exception {
        //监听已经存在的业务worker
        List<String> apps = operation.getWorkerAppList();
        apps.forEach(app -> workerChangeActionDefinition(app));

        //监听新增业务的worker
        operation.watchWorkerAppList(new TaskPipelineFunctionAppListListener() {
            @Override
            public void onAppAdd(String appName) {
                workerChangeActionDefinition(appName);
            }
        });
    }

    private void workerChangeActionDefinition(String app){
        try {
            operation.workerChangeActionDefinition(app);
        } catch (Exception e) {
            log.error("app:{} worker change action definition exception:{}",app,e);
        }
    }

    /**
     * 处理task app目录下的已存在的任务
     * @throws Exception
     */
    private void existingTaskActionDefinition() throws Exception {
        List<String> appList = operation.getTaskAppList();
        appList.forEach(app -> {
            try {
                operation.existingTaskActionDefinition(app);
            } catch (Exception e) {
                log.error("existing app:{} action init exception:{}",app,e);
            }
        });
    }


    /**
     * 处理assign app目录下已经存在的作业
     * @throws Exception
     */
    private void existingAssignTaskActionDefinition() throws Exception {
        List<String> appList = operation.getAssignTaskAppList();
        appList.forEach(app -> {
            try {
                operation.existingAssignTaskActionDefinition(app);
            } catch (Exception e) {
                log.error("existing assign app:{} action init exception:{}",app,e);
            }
        });
    }



}
