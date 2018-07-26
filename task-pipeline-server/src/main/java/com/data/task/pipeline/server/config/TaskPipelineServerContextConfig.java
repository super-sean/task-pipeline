package com.data.task.pipeline.server.config;

import com.data.task.pipeline.core.beans.TaskPipelineCoreConfig;
import com.data.task.pipeline.server.beans.TaskPipelineServerOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author xinzai
 * @create 2018-07-25 下午4:31
 **/
@Configuration
@EnableConfigurationProperties(TaskPipelineServerConfig.class)
public class TaskPipelineServerContextConfig {
    @Autowired
    private TaskPipelineServerConfig config;

    @Bean
    public TaskPipelineServerOperation getTaskPipelineServerOperation(){
        return new TaskPipelineServerOperation(new TaskPipelineCoreConfig(
                config.getZkConnectStr(),
                config.getSessionTimeout(),
                config.getBaseSleepTimeMs(),
                config.getMaxRetries(),
                config.getCorePoolSize(),
                config.getMaxthreadPoolSize(),
                config.getKeepApiveTime(),
                config.getQueueSize()
        ));
    }
}
