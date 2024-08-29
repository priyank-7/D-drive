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
                .username(document.getString("username"))
                .passwordHash(document.getString("passwordHash"))
                .role(document.getString("role"))
                .usedStorage(document.getDouble("usedStorage").floatValue())
                .build();
    }
}
