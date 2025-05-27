/*
 * Copyright (c) 2025 Sergei Shushkevich
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package com.whisperinggarden.lar;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@EnableConfigurationProperties(LiquibaseRollbackProperties.class)
@ConditionalOnProperty(prefix = "spring.liquibase-rollback", name = "enabled", matchIfMissing = true)
public class LiquibaseRollbackAutoConfiguration {

    @Bean
    public LiquibaseRollbackCustomizer liquibaseRollbackCustomizer(LiquibaseRollbackProperties properties) {
        return new LiquibaseRollbackCustomizer(properties);
    }
}
