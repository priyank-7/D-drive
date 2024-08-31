package com.cloude.headers;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Metadata implements Serializable {
    private String name;
    private long size;
    private boolean isFolder;
    private Date createdDate;
    private Date modifiedDate;
    private ObjectId owner;
    private List<ObjectId> sharedWith;
}
