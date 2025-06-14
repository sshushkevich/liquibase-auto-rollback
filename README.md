# Liquibase Auto Rollback Helper for Spring Boot

**Liquibase Auto Rollback Helper** is a lightweight helper library designed to simplify database changeset management in projects using **Spring Boot** and **Liquibase**.

## What It Does

Managing Liquibase changesets across feature branches and deployment environments can be a challenge - especially when switching between branches with different DB states. This library automatically **generates**, **stores**, and **executes rollback statements** for each changeset, making it easy to:

- Switch between branches with conflicting or missing Liquibase migrations
- Avoid manual rollback headaches or broken environments
- Ensure smoother CI/CD workflows and multi-branch deployments

## Key Features

- ✅ Automatically generates rollback SQL for new changesets during deployment
- ✅ Saves rollback statements in your database for later use
- ✅ Detects missing changesets when switching branches and automatically rolls them back
- ✅ Transparent integration with your Spring Boot application

## Use Case Example

1. A developer deploys a **feature branch** with new Liquibase changes to a **staging environment**.
2. The helper library generates and stores rollback statements for those changesets in the staging DB.
3. Later, the developer switches to the **`develop`** branch (which doesn’t include those changesets).
4. The library detects the missing changesets and **executes the saved rollbacks**, restoring the database to the correct state.

## License

This project is licensed under the [MIT License](LICENSE).
