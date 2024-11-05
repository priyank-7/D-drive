package com.cloude.headers;

public enum RequestType {
    AUTHENTICATE,
    FORWARD_REQUEST,
    UPLOAD_FILE,
    DOWNLOAD_FILE,
    GET_METADATA,
    DELETE_FILE,
    VALIDATE_TOKEN,
    DISCONNECT,
    REGISTER,
    UNREGISTER,
    PING,
    PUSH_DATA,
    PULL_DATA,
    DELETE_DATA,
    ACK_REPLICATION,
    ASK
}
