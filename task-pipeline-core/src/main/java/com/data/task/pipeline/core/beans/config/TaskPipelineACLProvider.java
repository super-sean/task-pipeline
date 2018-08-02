package com.data.task.pipeline.core.beans.config;

import org.apache.curator.framework.AuthInfo;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.ACL_AUTH_SCHEMA;
import static com.data.task.pipeline.core.beans.TaskPipelineCoreConstant.DIGEST;

/**
 * @author xinzai
 * create 2018-08-01 上午9:24
 **/
public class TaskPipelineACLProvider implements ACLProvider {

    private List<ACL> acls;

    private String authId;

    private List<AuthInfo> authInfos;

    public TaskPipelineACLProvider(List<String> aclIds,String authId) {
        this.authId = authId;
        this.authInfos = aclIds.stream().map(aclId -> new AuthInfo(DIGEST,aclId.getBytes())).collect(Collectors.toList());
    }

    @Override
    public List<ACL> getDefaultAcl() {
        if(acls ==null){
            this.acls = Arrays.asList(new ACL(ZooDefs.Perms.ALL, new Id(ACL_AUTH_SCHEMA, authId)));
        }
        return acls;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
        return acls;
    }

    public List<AuthInfo> getAuthInfos() {
        return authInfos;
    }

}
