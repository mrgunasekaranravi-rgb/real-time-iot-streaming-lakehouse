# Contributing

Thanks for your interest in this Real-Time IoT Streaming Lakehouse project.

## Contribution Guidelines

1. Fork the repository.
2. Create a feature branch.
3. Make your changes.
4. Validate Python syntax and project structure.
5. Commit changes with a clear message.
6. Push the branch.
7. Open a Pull Request.

## Development Standards

Contributions should follow these practices:

- Keep notebook naming consistent.
- Preserve the Bronze, Silver, and Gold architecture flow.
- Avoid committing secrets, credentials, or environment files.
- Maintain readable PySpark code.
- Keep data quality checks intact.
- Update documentation when project behavior changes.
- Ensure GitHub Actions CI passes before submitting changes.

## Project Validation

Before submitting a contribution, verify:

- Python syntax is valid.
- Required project files are present.
- README documentation is updated if needed.
- The pipeline validation logic remains consistent.
- CI workflow completes successfully.

## Code Style

Use clear and descriptive variable names.

Prefer readable DataFrame transformations and avoid unnecessary complexity.

Keep comments focused on engineering intent rather than obvious implementation details.

## Security

Do not commit:

- API keys
- Databricks tokens
- Credentials
- `.env` files
- Private connection information
- Production secrets

## Pull Requests

Pull Requests should include:

- A clear description of the change
- Why the change is required
- Any impact on pipeline behavior
- Validation performed
- Relevant screenshots if the output changes
