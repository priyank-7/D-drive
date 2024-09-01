package com.cloude.utilities;

import java.net.InetSocketAddress;
import java.util.Date;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Builder
@ToString
public class NodeInfo {

    private String nodeId;
    private NodeType nodetype;
    private InetSocketAddress nodeAddress;
    private NodeStatus status;
    private Date registrationTime;
    private Date lastResponse;
    private int failedAttempts;
    private int totalAttempts;
}
