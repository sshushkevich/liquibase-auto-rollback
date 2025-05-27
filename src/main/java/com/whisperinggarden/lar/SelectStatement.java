/*
 * Copyright (c) 2025 Sergei Shushkevich
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package com.whisperinggarden.lar;

import liquibase.statement.AbstractSqlStatement;
import liquibase.statement.core.DatabaseTableIdentifier;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Getter
public class SelectStatement extends AbstractSqlStatement {

    private final DatabaseTableIdentifier databaseTableIdentifier;
    private final List<String> columnsToSelect = new ArrayList<>();
    private String where;
    private final List<String> whereColumnNames = new ArrayList<>();
    private final List<Object> whereParameters = new ArrayList<>();
    private String orderBy;

    public SelectStatement(String catalogName, String schemaName, String tableName) {
        databaseTableIdentifier = new DatabaseTableIdentifier(catalogName, schemaName, tableName);
    }

    public String getCatalogName() {
        return databaseTableIdentifier.getCatalogName();
    }

    public String getSchemaName() {
        return databaseTableIdentifier.getSchemaName();
    }

    public String getTableName() {
        return databaseTableIdentifier.getTableName();
    }

    public SelectStatement addColumnsToSelect(String... columnNames) {
        Collections.addAll(columnsToSelect, columnNames);
        return this;
    }

    public SelectStatement setWhere(String where) {
        this.where = where;
        return this;
    }

    public SelectStatement addWhereColumnNames(String... columnNames) {
        Collections.addAll(whereColumnNames, columnNames);
        return this;
    }

    public SelectStatement addWhereParameters(Object... parameters) {
        Collections.addAll(whereParameters, parameters);
        return this;
    }

    public SelectStatement setOrderBy(String orderBy) {
        this.orderBy = orderBy;
        return this;
    }
}
