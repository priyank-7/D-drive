package com.cloude.db;

import com.cloude.headers.Metadata;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;

import org.bson.Document;
import org.bson.types.ObjectId;

public class MetadataDao {
    private final MongoCollection<Document> collection;

    public MetadataDao(String dbName, String collectionName) {
        this.collection = MongoDBConnection.getCollection(dbName, collectionName);
    }

    public boolean saveMetadata(Metadata metadata) {
        Document doc = new Document("name", metadata.getName())
                .append("size", metadata.getSize())
                .append("path", metadata.getPath())
                .append("isFolder", metadata.isFolder())
                .append("createdDate", metadata.getCreatedDate())
                .append("modifiedDate", metadata.getModifiedDate())
                .append("owner", metadata.getOwner())
                .append("sharedWith", metadata.getSharedWith());
        collection.insertOne(doc);
        return true;
    }

    public boolean updateMetadata(String fileName, ObjectId ownerId, Metadata newMetadata) {
        Document filter = new Document("name", fileName).append("owner", ownerId);

        Document update = new Document("$set", new Document("name", newMetadata.getName())
                .append("size", newMetadata.getSize())
                .append("path", newMetadata.getPath())
                .append("isFolder", newMetadata.isFolder())
                .append("createdDate", newMetadata.getCreatedDate())
                .append("modifiedDate", newMetadata.getModifiedDate())
                .append("owner", newMetadata.getOwner())
                .append("sharedWith", newMetadata.getSharedWith()));

        // Perform the update
        UpdateResult result = collection.updateOne(filter, update);

        // Print or return the result
        if (result.getMatchedCount() > 0) {
            return true;
        } else {
            return false;
        }
    }

    public boolean deleteMetadata(String fileName, ObjectId ownerId) {
        collection.deleteOne(Filters.and(
                Filters.eq("name", fileName),
                Filters.eq("owner", ownerId)));
        return true;
    }

    public Metadata getMetadata(String fileName, ObjectId ownerId) {
        Document doc = collection.find(Filters.and(
                Filters.eq("name", fileName),
                Filters.eq("owner", ownerId))).first();

        if (doc != null) {
            return Metadata.builder()
                    .name(doc.getString("name"))
                    .size(doc.getLong("size"))
                    .path(doc.getString("path"))
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
