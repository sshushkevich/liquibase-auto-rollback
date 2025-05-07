package com.whisperinggarden.lar;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(LiquibaseRollbackProperties.class)
public class LiquibaseRollbackAutoConfiguration {

    @Bean
    public LiquibaseRollbackCustomizer liquibaseRollbackCustomizer(LiquibaseRollbackProperties properties) {
        return new LiquibaseRollbackCustomizer(properties);
    }
}
