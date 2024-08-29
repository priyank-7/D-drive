# 🌩️ Distributed Cloud Storage Project

## 📚 Overview

Welcome to the Distributed Cloud Storage Project! This system has the capabilities of popular cloud storage services like Google Drive, offering a solution for storing and managing files across multiple distributed nodes.

## ✨ Features

- **Distributed File Storage**: Files are stored across multiple storage nodes for redundancy and scalability.
- **Load Balancing**: Efficiently distributes client requests across available storage nodes.
- **User Authentication**: authentication mechanism before file operations.
- **File Operations**: Supports `UPLOAD`, `DOWNLOAD`, `LIST`, and `DELETE` commands.

## 🛠️ Technologies Used

- **Java**: Core programming language for the project.
- **TCP/IP**: Network protocol for communication.

## 🚀 Getting Started

### Prerequisites

- Java 17 or higher
- Maven

### Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/priyank-7/D-drive.git
   ```
   ```bash
   cd D-drive
   ```
   ```bash
   mvn clean install
   ```

## 📘 Usage

### Commands

- **Authenticate**: AUTH username:password
- **Upload File**: PUT /path/to/file.txt
- **Download File**: GET filename.txt
- **Delete File**: DELETE filename.txt
- **List Files**: LIST
- **Exit**: EXIT

## 🌟 Acknowledgements

Special thanks to all the developers and contributors who have made this project possible.
