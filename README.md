# GitHub Candidate Analyzer (Capstone Project)

## Tech Stack

* **Language:** Java 21
* **Framework:** Spring Boot 3.2.0
* **Messaging:** Apache Kafka (3-broker cluster, KRaft mode)
* **Stream Processing:** Kafka Streams (utilizing **Exactly Once Semantics v2**)
* **Testing:** JUnit 5, Mockito, Spring Kafka Test, TopologyTestDriver

---

## System Architecture

The project consists of two specialized Spring Boot modules:

### 1. github-producer-service
* **Source:** Reads candidate accounts from `accounts.json`.
* **Accounts Stream:** Publishes each account to the `github_accounts` topic.
* **Commit Ingestion:** Listens for accounts, fetches their commit history via GitHub API (based on intervals like `1d`, `1w`), and pushes individual commits to the `github_commits` topic.

### 2. streams-service
* **Data Processing:** Consumes the `github_commits` stream.
* **Analytics:** Performs real-time windowless stateful aggregation.
* **Reporting:** Exports **8 distinct metrics** to a local `metrics.txt` file every 10 seconds.

---

## Getting Started

### 1. Prerequisites
* Docker and Docker Compose installed.
* A valid **GitHub Personal Access Token (PAT)**.

### 2. Infrastructure Setup
Spin up the 3-node Kafka cluster and Kafka UI:

```bash
docker-compose up -d
```

You can monitor the cluster health, topics, and consumer groups in real-time at http://localhost:8080.

### 3. Execution Order
`github-producer-service`: Run this module first. It automatically initializes the required Kafka topics (github_accounts, github_commits) with a replication factor of 3 and 3 partitions.

`streams-service`: Run this module second. It will start consuming the commit stream and generating the analytical report.

---

## Analytics & Metrics
The system performs real-time processing and generates a report in `metrics.txt` every 10 seconds using the following format:

```
- Total Commits: %d
- Total Contributors: %d
- Top 5 Contributors:
  [Sorted List by Commit Count]
- Top Languages:
  [Count per Programming Language]

CUSTOM METRICS:
- Avg Message Length: %d characters
- Weekend Activity: %d commits
- Most Active Day:
  [Sorted Days of Week from Highest to Lowest activity]
- Total Repositories Scanned: %d
```
