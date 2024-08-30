package com.cloude.db;

import com.cloude.DBurl;
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
        ConnectionString connectionString = new ConnectionString(DBurl.DBurl);

        // Configure connection pooling settings
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToConnectionPoolSettings(builder -> builder.maxSize(50)
                        .minSize(10)
                        .maxConnectionIdleTime(60, java.util.concurrent.TimeUnit.SECONDS))
                .build();

        // Create MongoDB client
        mongoClient = MongoClients.create(settings);
        System.out.println("MongoDB connection initialized");
    }

    public static MongoDatabase getDatabase(String dbName) {
        System.out.println("Database name: " + dbName);
        return mongoClient.getDatabase(dbName);
    }

    public static MongoCollection<Document> getCollection(String dbName, String collectionName) {
        System.out.println("Collection name: " + collectionName);
        return getDatabase(dbName).getCollection(collectionName);
    }

    public static void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
