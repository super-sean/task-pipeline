package com.data.task.pipeline.core.beans;

/**
 * @author xinzai
 * @create 2018-07-23 下午3:51
 **/
public class TaskPipelineCoreConstant {
    public static final String NAMESPACE = "task_pipeline";
    public static final String MASTER_NODE = "/master";
    public static final String APPS_PATH = "/apps/";
    public static final String WORKERS_PATH = "/workers/";
    public static final String TASKS_PATH = "/tasks/";
    public static final String TASKS_PARAMS = "/params";
    public static final String TASKS_STATUS = "/status";
    public static final String TASKS_RESULT = "/result";
    public static final String ASSIGN_PATH = "/assign/";
    public static final String WORKER_INIT_WEIGHT = "0";
    public static final String ASSIGN_TASK_SEP = "__";
    public static final String HISTORY_DIR = "/history/";
    public static final String TASK_SEP = "-";
    public static final String WORKER = "worker";
    public static final String TASK = "task";
    public static final String APP_Node_NAME = "appNodeName";
    public static final String APP = "app";
    public static final String SERVER = "server";

    public enum TaskStatus {
        SUBMIT("submit"),RUNNING("running"),DONE("done"),CONSUMED("consumed"),NOWORKER("noworker"),REPEAT("repeat"),MISSAPP("missapp"),RESUBMIT("resubmit");
        private final String status;
        TaskStatus(String status){
            this.status = status;
        }
        public String status() {
            return status;
        }
    }
}
