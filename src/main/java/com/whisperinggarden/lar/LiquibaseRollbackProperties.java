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
