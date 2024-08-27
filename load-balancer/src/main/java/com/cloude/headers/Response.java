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
public class Response implements java.io.Serializable {
    private StatusCode statusCode;
    private Object payload;
    private byte[] data;
    private int dataSize;

    public Response(StatusCode statusCode, Object payload) {
        this.statusCode = statusCode;
        this.payload = payload;
    }
}