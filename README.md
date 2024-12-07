





# Plan Management System
# Overview
This project implements a  plan management system using FastAPI, Elasticsearch, Redis, and Kafka. It manages healthcare plans with complex nested structures including plan cost shares, linked services, and service cost shares using parent-child relationships in Elasticsearch.
System Architecture

FastAPI: REST API endpoints for CRUD operations
Elasticsearch: Document storage with parent-child relationships
Redis: Cache layer for quick plan lookups
Kafka: Message broker for asynchronous operations

# Data Structure
The system manages healthcare plans with the following hierarchy:
CopyPlan 
├── PlanCostShares 
└── LinkedPlanServices 
    ├── LinkedService 
    └── PlanServiceCostShares 
Parent-Child Relationships

Plan → PlanCostShares (1:1)
Plan → LinkedPlanServices (1:n)
LinkedPlanServices → LinkedService (1:1)
LinkedPlanServices → PlanServiceCostShares (1:1)

# Setup and Installation
# Prerequisites

Python 3.8+
Elasticsearch 7.x
Redis
Apache Kafka
FastAPI

# Installation Steps

Clone the repository
Install dependencies:

bashCopy `pip install fastapi elasticsearch kafka-python redis`

Start required services: `uvicorn main:app --reload`

bashCopy# Start Elasticsearch
`sudo systemctl start elasticsearch`

# Start Redis
`sudo systemctl start redis`

# Start Kafka
`bin/zookeeper-server-start.sh config/zookeeper.properties`
`bin/kafka-server-start.sh config/server.properties`

Create Kafka topic:

bashCopy `bin/kafka-topics.sh --create --topic plan_operations --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`

# API Endpoints
POST /v1/plan
Creates a new healthcare plan
bashCopy `curl -X POST http://localhost:8000/v1/plan -H "Content-Type: application/json" -d @plan.json`
GET /v1/plan/{objectId}
Retrieves a specific plan
bashCopycurl `http://localhost:8000/v1/plan/12xvxc345ssdsds-508`
DELETE /v1/plan/{objectId}
Deletes a plan and all related documents
bashCopy `curl -X DELETE http://localhost:8000/v1/plan/12xvxc345ssdsds-508`

Kafka Consumer
Monitor operations with:
bashCopy `bin/kafka-console-consumer.sh --topic plan_operations --bootstrap-server localhost:9092 --from-beginning`

# Error Handling

400: Invalid request body
404: Plan not found
409: Plan already exists
500: Internal server error

# Best Practices

Always use proper routing for parent-child operations
Handle cascade deletions carefully
Verify plan existence before operations
Use appropriate error handling
Implement proper validation

# Monitoring

Monitor Elasticsearch operations and indexing
Check Kafka consumer logs
Monitor Redis cache hits/misses
Track API response times

# Contributing

Fork the repository
Create a feature branch
Submit a pull request
