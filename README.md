# **MD5 Distributed Brute-Force**

This project is a distributed brute-force MD5 hash cracker implemented in Python. It utilizes multiprocessing and network communication between a central server and multiple clients to divide the computational workload of cracking an MD5 hash across several machines.

## **Objective:**

The objective of this project is to efficiently solve MD5 hashes using distributed computing. By dividing the workload between multiple clients, the brute-force process becomes faster, allowing for quicker solutions to MD5 hashes.

### **How It Works:**

- **MD5 Hash Cracking**: The system performs a brute-force attack to find the original string that produces a given MD5 hash.
- **Workload Distribution**: The server divides the keyspace (range of possible inputs) among connected clients. Each client works on a part of the keyspace to speed up the overall cracking process.
- **Communication**: The server communicates with the clients over a network, coordinating tasks and sending progress updates.

---

## **System Architecture:**

The project is built on a client-server model, where:

### **Server**:
- Distributes hash tasks and keyspaces to clients.
- Collects results and monitors progress.
- Determines when a match is found or when all keyspaces have been exhausted.

### **Client**:
- Receives a keyspace to test from the server.
- Iterates over possible strings, computes the MD5 hash for each, and checks if it matches the target hash.
- Sends results or updates back to the server.

---

## **Sequence Diagram + Protocol:**

### **Sequence Diagram:**
![sequence_diagram](sequence_diagram_md5_brute.jpeg?raw=true "Title")

### **Explanation and Protocol:**

#### **TCP**:
- **Client -> Server**: Request task, send progress, report result.
- **Server -> Client**: Assign keyspace, send results, declare success or failure.

#### **Message Structure**:
      Each TCP message follows a predefined format:
      ```plaintext
      [OPERATION_CODE | DATA]
      ```
      - `OPERATION_CODE`: Indicates the type of message being sent (e.g., `TASK_ASSIGNMENT`, `RESULT`, `SUCCESS`).
      - `DATA`: Any associated data with the operation (e.g., start/end of keyspace, result string).
---

## **Installation and Usage**
*To use the MD5 Distributed Brute-Force tool, follow these steps:*

1. Clone the repository:  
   ```bash
   git clone https://github.com/EniacARC/md5_distributed_brute.git

## **Credits**
This project was created by Yonathan Chapal.
