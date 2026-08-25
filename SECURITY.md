# Security Policy

## Supported Versions

This repository is a portfolio and learning project demonstrating a Real-Time IoT Streaming Lakehouse architecture.

Security updates are applied to the latest version of the project.

## Reporting a Vulnerability

If you discover a security issue, please avoid creating a public issue containing sensitive details.

Instead, report the problem privately to the repository owner.

Please include:

- A clear description of the issue
- Steps to reproduce it
- Potential impact
- Suggested remediation, if available

## Secrets and Credentials

Do not commit:

- API keys
- Databricks access tokens
- Cloud credentials
- Database passwords
- `.env` files
- Private connection strings
- Production secrets

Sensitive configuration should be stored securely using environment variables or an appropriate secret-management service.

## Dependency Security

Project dependencies should be reviewed periodically for known vulnerabilities.

GitHub security features and dependency alerts may be used where applicable.

## Scope

This project is intended to demonstrate data engineering and streaming architecture patterns.

It is not intended to represent a fully production-hardened security implementation.
