package com.cloude.db;

import java.io.Serializable;

import org.bson.types.ObjectId;
import java.util.*;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class User implements Serializable {
    private ObjectId _id;
    private String username;
    private String passwordHash;
    private String role;
    private float usedStorage;
    private List<ObjectId> sharedOjectIds;

    public User(String username, String passwordHash, String role) {
        this.username = username;
        this.passwordHash = passwordHash;
        this.role = role;
    }
}