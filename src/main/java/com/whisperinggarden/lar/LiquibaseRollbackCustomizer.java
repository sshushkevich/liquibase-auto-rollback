package com.whisperinggarden.lar;

import liquibase.Liquibase;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.integration.spring.Customizer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class LiquibaseRollbackCustomizer implements Customizer<Liquibase> {

    private final LiquibaseRollbackProperties properties;

    @Override
    public void customize(Liquibase liquibase) {
        log.info("Starting database auto-rollback processing");

        try {
            LiquibaseRollbackUtils.createRollbackTable(liquibase.getDatabase(), properties.getDbRollbackTableName());
        } catch (DatabaseException e) {
            throw new UnexpectedLiquibaseException(e.getMessage());
        }

        log.info("Database auto-rollback processing completed successfully");
    }
}
