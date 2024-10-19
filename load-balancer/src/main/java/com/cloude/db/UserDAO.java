package com.cloude.db;

import com.cloude.db.mappers.UserMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Filters.eq;

public class UserDAO {

    private final MongoCollection<Document> userCollection;

    public UserDAO(MongoDatabase database) {
        this.userCollection = database.getCollection("user");
    }

    public void insertUser(User user) {
        Document userDoc = UserMapper.toDocument(user);
        userCollection.insertOne(userDoc);
    }

    public User getUserByUsername(String username) {
        Document doc = userCollection.find(eq("username", username)).first();
        if (doc != null) {
            return UserMapper.fromDocument(doc);
        } else {
            return null;
        }
    }
}
