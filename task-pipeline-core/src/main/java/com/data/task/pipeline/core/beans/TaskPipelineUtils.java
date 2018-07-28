package com.data.task.pipeline.core.beans;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author xinzai
 * @create 2018-07-27 上午10:32
 **/
public class TaskPipelineUtils {
    private static Logger log = LoggerFactory.getLogger(TaskPipelineUtils.class);

    public static String getLocalNodeName(){
        try {
            InetAddress addr = InetAddress.getLocalHost();
            //获取本机ip
            String ip=addr.getHostAddress().toString();
            //获取本机计算机名称
            String hostName=addr.getHostName().toString();
            return hostName + "-" + ip + "-" + System.currentTimeMillis();
        } catch (UnknownHostException e) {
            log.warn("get host info exception",e);
        }
        return System.currentTimeMillis() + "";
    }
}
