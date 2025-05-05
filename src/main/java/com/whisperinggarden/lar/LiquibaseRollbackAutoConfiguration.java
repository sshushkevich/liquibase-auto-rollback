package com.whisperinggarden.lar;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class LiquibaseRollbackAutoConfiguration {

    @Bean
    public LiquibaseRollbackCustomizer liquibaseRollbackCustomizer() {
        return new LiquibaseRollbackCustomizer();
    }
}
