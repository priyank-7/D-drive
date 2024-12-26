# D-Drive: Distributed Cloud Storage System

## Overview

D-Drive is a distributed storage system designed to provide scalable, secure, and efficient file storage across multiple nodes. It ensures high availability, fault tolerance, and seamless user experience for uploading, downloading, and managing files.

## Features

- **File Operations**: Supports file upload, download, and deletion.
- **Data Replication**: Ensures redundancy by replicating files across storage nodes.
- **Load Balancing**: Distributes user requests across storage nodes using a round-robin algorithm.
- **Authentication**: Provides token-based authentication for secure access.
- **Service Registry**: Tracks and monitors active storage nodes and load balancers.
- **Fault Tolerance**: Uses heartbeats to detect node failures and dynamically redistributes responsibilities.

## File Storage and Download Details

### File Upload

- **Storage Location**:
  - Files uploaded by users are stored in the home directory of the logged-in user under the `/ddrive-storage` directory.
  - Once uploaded, the file is automatically replicated to all other active storage nodes in the system to ensure redundancy and fault tolerance.

### File Download

- **Download Location on Client Device**:
  - Files downloaded by the client are saved in the **`Downloads`** folder of the client device.

## Architecture

The system comprises the following components:

1. **Client**: Issues commands like upload, download, and delete to interact with the system.
2. **Load Balancer**: Distributes client requests to storage nodes and validates authentication tokens.
3. **Storage Nodes**: Store files and handle replication, ensuring data availability.
4. **Service Registry**: Tracks live nodes, manages metadata, and facilitates communication between components.
5. **Database**: Stores metadata, user information, and access control details.

![Architecture Diagram](/Diagrams/System_Overview.png)

## File Storage Mechanism

Files are stored on all ther storage nodes and replicated across all storage nodes for redundancy. Metadata associated with files is managed by the database.

## Platforms and Technologies

### Software

- **Programming Language**: Java
- **Database**: MongoDB
- **Tools**: Maven
- **Logging**: Log4j
- **Libraries**: JWT for authentication, MongoDB Driver

## Installation

#### 1. Clone the repository:

```bash
git clone https://github.com/username/d-drive.git
cd d-drive
```

#### 2. Build and Run Modules

##### 1. Service Registry

```bash
cd service-registry
mvn clean install
java -jar target/service-registory-1.0-SNAPSHOT.jar <registry-port>
```

Registory Will run on `<registry-port>`

##### 2. Load Balancer

```bash
cd ../load-balancer
mvn clean install
java -jar target/load-balancer-1.0-SNAPSHOT.jar <registry-ip> <registry-port> <loadBalancer-port>
```

Load Balancer Will run on `<loadBalancer-port>`

##### 3. Storage Node

```bash
cd ../storage-node
mvn clean install
java -jar target/server-1.0-SNAPSHOT.jar <registry-ip> <registry-port> <storageNode-port>
```

Storage Node Will run on `<storageNode-port>`, can spin up multiple Storage Nodes with different ports.

##### 4. Client

```bash
cd ../client
mvn clean install
java -jar target/client-1.0-SNAPSHOT.jar <loadBalancer-ip> <loadBalancer-port>
```

### Commands

- **`AUTH username:password`**: Login
- **`PUT File_Path`**: Upload a file to the storage system.
- **`GET File_Name`**: Retrieve a file from the system.
- **`Delete File_Name`**: Remove a file from the storage system.
- **`EXIT`**: Exit the client program

## Results

- Successfully supports concurrent file operations.
- Provides consistent replication and data integrity.
- Handles node failures gracefully without disrupting user operations.

## Future Work

- Implement end-to-end encryption for enhanced security.
- Add a user-friendly web-based UI.
- Introduce dynamic load balancing
