package com.cloude.db;

import com.cloude.headers.Metadata;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.types.ObjectId;

public class MetadataDao {
    private final MongoCollection<Document> collection;

    public MetadataDao(String dbName, String collectionName) {
        this.collection = MongoDBConnection.getCollection(dbName, collectionName);
    }

    public void saveMetadata(Metadata metadata) {
        Document doc = new Document("name", metadata.getName())
                .append("size", metadata.getSize())
                .append("isFolder", metadata.isFolder())
                .append("createdDate", metadata.getCreatedDate())
                .append("modifiedDate", metadata.getModifiedDate())
                .append("owner", metadata.getOwner())
                .append("sharedWith", metadata.getSharedWith());
        collection.insertOne(doc);
        System.out.println("Metadata saved: " + metadata.getName());
    }

    public void deleteMetadata(String fileName, ObjectId ownerId) {
        collection.deleteOne(Filters.and(
                Filters.eq("name", fileName),
                Filters.eq("owner", ownerId)));
        System.out.println("Metadata deleted for file: " + fileName);
    }

    public Metadata getMetadata(String fileName, ObjectId ownerId) {
        Document doc = collection.find(Filters.and(
                Filters.eq("name", fileName),
                Filters.eq("owner", ownerId))).first();

        if (doc != null) {
            return Metadata.builder()
                    .name(doc.getString("name"))
                    .size(doc.getLong("size"))
                    .isFolder(doc.getBoolean("isFolder"))
                    .createdDate(doc.getDate("createdDate"))
                    .modifiedDate(doc.getDate("modifiedDate"))
                    .owner(doc.getObjectId("owner"))
                    .sharedWith(doc.getList("sharedWith", ObjectId.class))
                    .build();
        } else {
            System.out.println("Metadata not found for file: " + fileName);
            return null;
        }
    }
}
