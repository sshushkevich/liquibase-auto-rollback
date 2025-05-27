/*
 * Copyright (c) 2025 Sergei Shushkevich
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package com.whisperinggarden.lar;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.liquibase-rollback", ignoreUnknownFields = false)
@Getter
@Setter
public class LiquibaseRollbackProperties {

    private boolean enabled;

    private String dbChangeLogTable = "DATABASECHANGELOG";

    private String dbRollbackTableName = "DATABASECHANGELOGRB";

    private int rollbackStatementMaxLength = 4096;
}
