package com.data.task.pipeline.core.beans;

/**
 * @author xinzai
 * @create 2018-07-23 下午3:51
 **/
public class TaskPipelineCoreConstant {
    public static final String NAMESPACE = "task_pipeline";
    public static final String MASTER_PATH = "/master/";
    public static final String APPS_PATH = "/apps/";
    public static final String WORKERS_PATH = "/workers/";
    public static final String TASKS_PATH = "/tasks/";
    public static final String TASKS_PARAMS = "/params";
    public static final String TASKS_STATUS = "/status";
    public static final String TASKS_RESULT = "/result";
    public static final String ASSIGN_PATH = "/assign/";
    public static final String WORKER_INIT_WEIGHT = "0";
    public static final String ASSIGN_TASK_SEP = "__";
    public static final String TASK_SEP = "-";

    public enum TaskStatus {
        SUBMIT("submit"),RUNNING("running"),DONE("done");
        private final String status;
        TaskStatus(String status){
            this.status = status;
        }
        public String status() {
            return status;
        }
    }
}
