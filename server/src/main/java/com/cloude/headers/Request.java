package com.cloude.headers;

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
    private String token;
    private Object payload;
    private byte[] data;
    private int dataSize;

    public Request(RequestType requestType) {
        this.requestType = requestType;
    }
}
