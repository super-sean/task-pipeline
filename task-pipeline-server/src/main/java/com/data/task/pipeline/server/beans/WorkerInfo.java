package com.data.task.pipeline.server.beans;

/**
 * @author xinzai
 * @create 2018-07-29 下午9:06
 **/
public class WorkerInfo {
    private String node;
    private Integer weight;

    public WorkerInfo(String node, Integer weight) {
        this.node = node;
        this.weight = weight;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }
}
