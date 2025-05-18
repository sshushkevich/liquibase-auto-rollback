package com.whisperinggarden.lar;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

import java.util.stream.Collectors;

import static liquibase.util.SqlUtil.replacePredicatePlaceholders;

public class SelectGenerator extends AbstractSqlGenerator<SelectStatement> {

    @Override
    public ValidationErrors validate(SelectStatement statement, Database database,
            SqlGeneratorChain<SelectStatement> sqlGeneratorChain) {

        var validationErrors = new ValidationErrors();
        validationErrors.checkRequiredField("tableName", statement.getTableName());
        validationErrors.checkRequiredField("columnsToSelect", statement.getColumnsToSelect());
        if (statement.getWhere() == null) {
            if (!statement.getWhereColumnNames().isEmpty()) {
                validationErrors.addError("whereColumnNames set but no where clause");
            }
            if (!statement.getWhereParameters().isEmpty()) {
                validationErrors.addError("whereParameters set but no where clause");
            }
        }
        return validationErrors;
    }

    @Override
    public Sql[] generateSql(SelectStatement statement, Database database,
            SqlGeneratorChain<SelectStatement> sqlGeneratorChain) {

        var catalog = statement.getCatalogName();
        var schema = statement.getSchemaName();
        var table = statement.getTableName();

        var sql = new StringBuilder("SELECT ")
                .append(statement.getColumnsToSelect().stream()
                        .map(name -> database.escapeColumnName(catalog, schema, table, name))
                        .collect(Collectors.joining(", ")))
                .append(" FROM ")
                .append(database.escapeTableName(catalog, schema, table));

        if (statement.getWhere() != null) {
            sql.append(" WHERE ").append(replacePredicatePlaceholders(database,
                    statement.getWhere(), statement.getWhereColumnNames(), statement.getWhereParameters()));
        }

        if (statement.getOrderBy() != null) {
            sql.append(" ORDER BY ").append(database.escapeColumnName(catalog, schema, table, statement.getOrderBy()));
        }

        return new Sql[] { new UnparsedSql(sql.toString(), getAffectedTable(statement)) };
    }

    protected Relation getAffectedTable(SelectStatement statement) {
        return new Table().setName(statement.getTableName())
                .setSchema(statement.getCatalogName(), statement.getSchemaName());
    }
}
