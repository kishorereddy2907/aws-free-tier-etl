
# AWS Serverless ETL Pipeline (Free Tier)

## Overview

This repository contains a **production-style, fully serverless ETL pipeline on AWS**, designed to run entirely within AWS Free Tier limits.

The pipeline demonstrates **event-driven architecture, decoupled services, secure CI/CD, and fault tolerance**, implemented using Infrastructure as Code (CloudFormation) and GitHub Actions with OIDC authentication.

This project was built to reflect **real enterprise data engineering patterns**, not toy examples.

---

## Architecture

### High-level flow

```
EventBridge (Schedule)
        ↓
Step Functions
        ↓
Validation Lambda
        ↓
SNS (Fan-out)
        ↓
SQS (Buffer + DLQ)
        ↓
Transformation Lambda
        ↓
Curated S3
```

### Key characteristics

* Fully serverless
* Asynchronous and decoupled
* Retryable and fault-tolerant
* No long-lived credentials
* Free-tier safe

---

## AWS Services Used

| Service            | Purpose                             |
| ------------------ | ----------------------------------- |
| Amazon S3          | Raw and curated data storage        |
| AWS Lambda         | Validation and transformation logic |
| AWS Step Functions | Workflow orchestration              |
| Amazon SNS         | Event fan-out                       |
| Amazon SQS         | Buffering, retries, DLQ             |
| Amazon EventBridge | Scheduling                          |
| AWS IAM            | Least-privilege access control      |
| AWS CloudFormation | Infrastructure as Code              |
| GitHub Actions     | CI/CD                               |
| GitHub OIDC        | Secure AWS authentication           |

---

## Detailed Workflow

1. **EventBridge** triggers the pipeline on a schedule.
2. **Step Functions** starts the workflow.
3. **Validation Lambda** validates incoming records.
4. Validation results are published to **SNS**.
5. **SNS** delivers messages to **SQS**.
6. **SQS** triggers the **Transformation Lambda**.
7. Transformed output is written to **curated S3**.
8. Failed messages are retried and eventually sent to **DLQ**.

---

## Failure Handling Strategy

* Lambda retry policies handle transient failures.
* Step Functions retries Lambda invocations.
* SQS redrive policy moves poison messages to DLQ.
* No silent failures, no data loss.

This design ensures **resilience without tight coupling**.

---

## Security Design

* **No hard-coded AWS credentials**
* GitHub Actions authenticates to AWS using **OIDC**
* Short-lived IAM role assumption
* Least-privilege IAM policies per service

This follows modern cloud security best practices.

---

## Infrastructure as Code

All infrastructure is provisioned using **AWS CloudFormation (YAML only)**.

Key resources:

* S3 buckets
* SNS topic
* SQS queue + DLQ
* Lambda functions
* IAM roles and permissions
* Step Functions state machine
* EventBridge schedule

Deployments are **idempotent and repeatable**.

---

## CI/CD Pipeline

* Triggered on `main` branch push
* Uses GitHub Actions (Free Tier)
* Authenticates via GitHub → AWS OIDC
* Deploys CloudFormation stacks via AWS CLI

No secrets stored in GitHub.

---

## Repository Structure

```
aws-free-tier-etl/
│
├── cloudformation/
│   └── etl-stack.yaml
│
├── lambdas/
│   ├── validation/
│   └── transformation/
│
├── stepfunctions/
│   └── state-machine.json
│
├── .github/
│   └── workflows/
│       └── deploy.yaml
│
└── README.md
```

---

## Cost Considerations

* No AWS Glue or EMR
* Low-frequency EventBridge schedule
* Small Lambda execution time
* SQS batch size tuned for predictability

Designed explicitly to remain **within AWS Free Tier limits**.

---

## Design Decisions (Interview-Ready)

* **Step Functions** for orchestration, not business logic
* **SNS + SQS** for decoupling and buffering
* **Lambda-only transformations** for low to medium data volumes
* **OIDC-based CI/CD** for security
* **CloudFormation** for deterministic infrastructure

Each service has a clear, justified role.

---

## Possible Enhancements

* Schema versioning and contract enforcement
* Idempotency keys for exactly-once processing
* CloudWatch alarms and metrics
* Parameterization for multi-environment deployments
* Migration to Glue or Spark for higher data volumes

---

## Author

**Kishore Reddy**
Cloud Data Engineer
AWS | Serverless | Event-Driven Data Pipelines

---
