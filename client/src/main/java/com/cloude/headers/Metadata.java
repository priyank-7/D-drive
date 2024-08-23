package com.cloude.headers;

import java.util.Date;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Metadata implements java.io.Serializable {

    private String name;
    private long size;
    private boolean isFolder;
    private Date createdDate;
    private Date modifiedDate;
    private String owner;
}
