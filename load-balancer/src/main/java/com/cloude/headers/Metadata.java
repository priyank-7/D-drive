package com.cloude.headers;

import java.util.*;

import org.bson.types.ObjectId;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Metadata implements java.io.Serializable {

    private String name;
    private long size;
    private String path;
    private boolean isFolder;
    private Date createdDate;
    private Date modifiedDate;
    private ObjectId owner;
    private List<ObjectId> sharedWith;
}
