package com.cloude.headers;

import java.net.InetSocketAddress;

import com.cloude.utilities.NodeType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Builder
public class Request implements java.io.Serializable {

    private RequestType requestType;
    private NodeType nodeType;
    private InetSocketAddress socketAddress;
    private Object payload;

    public Request(RequestType requestType, InetSocketAddress socketAddress, NodeType nodeType) {
        this.requestType = requestType;
        this.socketAddress = socketAddress;
        this.nodeType = nodeType;
    }

    public Request(RequestType requestType) {
        this.requestType = requestType;
    }
}