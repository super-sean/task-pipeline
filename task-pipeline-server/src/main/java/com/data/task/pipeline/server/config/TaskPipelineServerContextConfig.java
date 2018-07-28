package com.data.task.pipeline.server.config;

import com.data.task.pipeline.core.beans.config.TaskPipelineCoreConfig;
import com.data.task.pipeline.server.beans.TaskPipelineServerOperation;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author xinzai
 * @create 2018-07-25 下午4:31
 **/
@Configuration
@ConfigurationProperties(prefix = "task.pipeline")
@EnableConfigurationProperties(TaskPipelineServerContextConfig.class)
public class TaskPipelineServerContextConfig {

    private TaskPipelineCoreConfig config;

    @Bean
    public TaskPipelineServerOperation getTaskPipelineServerOperation(){
        return new TaskPipelineServerOperation(config);
    }

    public TaskPipelineCoreConfig getConfig() {
        return config;
    }

    public void setConfig(TaskPipelineCoreConfig config) {
        this.config = config;
    }
}
