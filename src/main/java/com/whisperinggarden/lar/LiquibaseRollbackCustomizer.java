package com.whisperinggarden.lar;

import liquibase.Liquibase;
import liquibase.integration.spring.Customizer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.whisperinggarden.lar.LiquibaseRollbackUtils.createRollbackTable;
import static com.whisperinggarden.lar.LiquibaseRollbackUtils.persistRollbackStatements;

@Slf4j
@RequiredArgsConstructor
public class LiquibaseRollbackCustomizer implements Customizer<Liquibase> {

    private final LiquibaseRollbackProperties properties;

    @Override
    public void customize(Liquibase liquibase) {
        log.info("Starting database auto-rollback processing");

        createRollbackTable(liquibase.getDatabase(), properties.getDbRollbackTableName());
        persistRollbackStatements(liquibase, properties.getDbRollbackTableName());

        log.info("Database auto-rollback processing completed successfully");
    }
}
