package com.cloude.db;

import com.cloude.db.mappers.MetadataMapper;
import com.cloude.db.mappers.UserMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

public class MetadataDAO {

    private MongoCollection<Document> userCollection;
    private MongoCollection<Document> metadataCollection;

    public MetadataDAO(MongoDatabase database) {
        this.userCollection = database.getCollection("users");
        this.metadataCollection = database.getCollection("metadata");
    }

    public ObjectId insertUser(User user) {
        Document document = UserMapper.toDocument(user);
        userCollection.insertOne(document);
        return document.getObjectId("_id"); // Return the generated ObjectId
    }

    public void insertMetadata(Metadata metadata) {
        Document document = MetadataMapper.toDocument(metadata);
        metadataCollection.insertOne(document);
    }

    public Metadata getMetadataByName(String name) {
        Document query = new Document("name", name);
        Document result = metadataCollection.find(query).first();
        if (result != null) {
            return MetadataMapper.fromDocument(result);
        }
        return null;
    }
}
