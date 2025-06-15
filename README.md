# Liquibase Auto Rollback Helper for Spring Boot

[![Maven Central](https://img.shields.io/maven-central/v/com.whisperinggarden/liquibase-auto-rollback.svg)](https://central.sonatype.com/artifact/com.whisperinggarden/liquibase-auto-rollback)

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

## Quick Start

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.whisperinggarden</groupId>
    <artifactId>liquibase-auto-rollback</artifactId>
    <version>1.0.2</version>
</dependency>
```

## Requirements

- Java 17+
- Liquibase 4.29.2+
- Spring Boot 3.4+

## Use Case Example

1. A developer deploys a **feature branch** with new Liquibase changes to a **staging environment**.
2. The helper library generates and stores rollback statements for those changesets in the staging DB.
3. Later, the developer switches to the **`develop`** branch (which doesn’t include those changesets).
4. The library detects the missing changesets and **executes the saved rollbacks**, restoring the database to the correct state.

## Configuration Properties

You can customize the behavior of the rollback helper using the following Spring Boot properties:

| Property Name                                            | Default Value        | Description                                                                 |
|----------------------------------------------------------|----------------------|-----------------------------------------------------------------------------|
| spring.liquibase-rollback.enabled                        | true                 | Enables or disables the rollback helper.                                    |
| spring.liquibase-rollback.db-rollback-table-name         | DATABASECHANGELOGRB  | Name of the table where rollback statements are stored.                     |
| spring.liquibase-rollback.rollback-statement-max-length  | 4096                 | Maximum length (in characters) of a stored rollback SQL statement.          |
| spring.liquibase-rollback.db-change-log-table            | DATABASECHANGELOG    | Name of the Liquibase changelog table.                                      |

## License

This project is licensed under the [MIT License](LICENSE).
