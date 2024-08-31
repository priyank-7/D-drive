package com.cloude.db.mappers;

import org.bson.Document;

import com.cloude.db.User;

public class UserMapper {

    public static Document toDocument(User user) {
        return new Document("username", user.getUsername())
                .append("passwordHash", user.getPasswordHash())
                .append("role", user.getRole())
                .append("usedStorage", user.getUsedStorage());
    }

    public static User fromDocument(Document document) {
        return User.builder()
                ._id(document.getObjectId("_id"))
                .username(document.getString("username"))
                .passwordHash(document.getString("passwordHash")) // remove if not needed
                .role(document.getString("role"))
                .usedStorage(document.getDouble("usedStorage").floatValue())
                .build();
    }
}
