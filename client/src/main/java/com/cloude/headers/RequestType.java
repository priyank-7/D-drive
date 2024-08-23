package com.cloude.headers;

public enum RequestType {
    AUTHENTICATE, // Request for user authentication
    FORWARD_REQUEST, // Request to forward to a storage node
    UPLOAD_FILE, // Request to upload a file to a storage node
    DOWNLOAD_FILE, // Request to download a file from a storage node
    GET_METADATA, // Request to retrieve metadata (file/folder)
    DELETE_FILE // Request to delete a file
}
