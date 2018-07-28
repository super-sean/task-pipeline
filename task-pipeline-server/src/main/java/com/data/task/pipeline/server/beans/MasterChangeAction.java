package com.data.task.pipeline.server.beans;

/**
 * @author xinzai
 * @create 2018-07-28 下午3:06
 **/
@FunctionalInterface
public interface MasterChangeAction {
    void onMasterChange() throws Exception ;
}
