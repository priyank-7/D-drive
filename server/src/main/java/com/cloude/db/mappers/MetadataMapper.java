package com.cloude.db.mappers;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.cloude.headers.Metadata;

import java.util.List;

public class MetadataMapper {

    public static Document toDocument(Metadata metadata) {
        return new Document("name", metadata.getName())
                .append("size", metadata.getSize())
                .append("isFolder", metadata.isFolder())
                .append("createdDate", metadata.getCreatedDate())
                .append("modifiedDate", metadata.getModifiedDate())
                .append("owner", metadata.getOwner())
                .append("sharedWith", metadata.getSharedWith());
    }

    @SuppressWarnings("unchecked")
    public static Metadata fromDocument(Document document) {
        return Metadata.builder()
                .name(document.getString("name"))
                .size(document.getLong("size"))
                .isFolder(document.getBoolean("isFolder"))
                .createdDate(document.getDate("createdDate"))
                .modifiedDate(document.getDate("modifiedDate"))
                .owner(document.getObjectId("owner"))
                .sharedWith((List<ObjectId>) document.get("sharedWith"))
                .build();
    }
}
