package com.cloude.utilities;

import java.net.InetSocketAddress;

import com.cloude.headers.RequestType;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ReplicateRequest {

    private String filePath;
    private InetSocketAddress address;
    private RequestType requestType;

}
