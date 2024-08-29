package com.cloude.db;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class MongoDBConnection {

    private static MongoClient mongoClient;

    static {
        // Configure MongoDB connection string
        ConnectionString connectionString = new ConnectionString(
                "mongodb+srv://priyankpatel9413:s7V7a9Jo6e10dCpR@ddrive.yamt4.mongodb.net/?retryWrites=true&w=majority&appName=Ddrive");

        // Configure connection pooling settings
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToConnectionPoolSettings(builder -> builder.maxSize(50)
                        .minSize(10)
                        .maxConnectionIdleTime(60, java.util.concurrent.TimeUnit.SECONDS))
                .build();

        // Create MongoDB client
        mongoClient = MongoClients.create(settings);
    }

    public static MongoDatabase getDatabase(String dbName) {
        return mongoClient.getDatabase(dbName);
    }

    public static MongoCollection<Document> getCollection(String dbName, String collectionName) {
        return getDatabase(dbName).getCollection(collectionName);
    }

    public static void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
