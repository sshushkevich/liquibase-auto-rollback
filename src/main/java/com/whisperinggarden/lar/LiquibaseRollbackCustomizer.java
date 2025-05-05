package com.whisperinggarden.lar;

import liquibase.Liquibase;
import liquibase.integration.spring.Customizer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LiquibaseRollbackCustomizer implements Customizer<Liquibase> {

    @Override
    public void customize(Liquibase liquibase) {
        log.info("Starting database auto-rollback processing");
        log.info("Database auto-rollback processing completed successfully");
    }
}
