/*
 * Copyright (c) 2025 Sergei Shushkevich
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package com.whisperinggarden.lar;

import liquibase.ChecksumVersion;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.Scope;
import liquibase.change.AddColumnConfig;
import liquibase.change.core.RawSQLChange;
import liquibase.changelog.RanChangeSet;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.RollbackImpossibleException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.integration.spring.Customizer;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.AutoIncrementConstraint;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateIndexStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.DeleteStatement;
import liquibase.statement.core.InsertStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.core.Column;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.stream.Stream;

import static com.whisperinggarden.lar.LiquibaseRollbackUtils.hasTable;
import static java.util.function.Predicate.not;

@Slf4j
@RequiredArgsConstructor
public class LiquibaseRollbackCustomizer implements Customizer<Liquibase> {

    // DATABASECHANGELOGRB table columns:
    public static final String COL_ID = "ID";
    public static final String COL_CHANGELOG_ID = "CHANGELOGID";
    public static final String COL_CHANGELOG_CHECKSUM = "CHANGELOGCHKSUM";
    public static final String COL_ROLLBACKSTMT = "ROLLBACKSTMT";
    public static final String COL_ROLLBACKSTMTORDER = "ROLLBACKSTMTORDER";

    // DATABASECHANGELOG table columns:
    public static final String COL_DBCHANGELOG_ID = "ID";
    public static final String COL_DBCHANGELOG_MD5SUM = "MD5SUM";

    private final LiquibaseRollbackProperties properties;

    @Override
    public void customize(Liquibase liquibase) {
        log.info("Starting database auto-rollback processing");

        createRollbackTable(liquibase.getDatabase());
        rollbackUnexpectedChangeSets(liquibase);
        persistRollbackStatements(liquibase);

        log.info("Database auto-rollback processing completed successfully");
    }

    protected void createRollbackTable(Database db) {
        var tableName = properties.getDbRollbackTableName();
        if (hasTable(db, tableName)) {
            return;
        }

        var executor = getExecutor(db);
        var intType = DataTypeFactory.getInstance().fromDescription("int", db);
        var varchar255Type = DataTypeFactory.getInstance().fromDescription("varchar(255)", db);
        var varchar100Type = DataTypeFactory.getInstance().fromDescription("varchar(100)", db);
        var varchar4KType = DataTypeFactory.getInstance()
                .fromDescription("varchar(%d)".formatted(properties.getRollbackStatementMaxLength()), db);

        log.info("Creating {} table", tableName);

        try {
            executor.execute(new CreateTableStatement(db.getLiquibaseCatalogName(), db.getLiquibaseSchemaName(), tableName)
                    .addPrimaryKeyColumn(COL_ID, intType, null, null, null, new AutoIncrementConstraint(COL_ID))
                    .addColumn(COL_CHANGELOG_ID, varchar255Type, new NotNullConstraint(COL_CHANGELOG_ID))
                    .addColumn(COL_CHANGELOG_CHECKSUM, varchar100Type, new NotNullConstraint(COL_CHANGELOG_CHECKSUM))
                    .addColumn(COL_ROLLBACKSTMT, varchar4KType, new NotNullConstraint(COL_ROLLBACKSTMT))
                    .addColumn(COL_ROLLBACKSTMTORDER, intType, new NotNullConstraint(COL_ROLLBACKSTMTORDER)));

            executor.execute(new CreateIndexStatement("IDX_RB_CHANGELOGIDSUMORD",
                    db.getLiquibaseCatalogName(), db.getLiquibaseSchemaName(), tableName, true, null,
                    new AddColumnConfig(new Column(COL_CHANGELOG_ID)),
                    new AddColumnConfig(new Column(COL_CHANGELOG_CHECKSUM)),
                    new AddColumnConfig(new Column(COL_ROLLBACKSTMTORDER))));

            db.commit();
        } catch (DatabaseException e) {
            throw new UnexpectedLiquibaseException("Unable to create the rollback table", e);
        }
    }

    protected void rollbackUnexpectedChangeSets(Liquibase liquibase) {
        var tableName = properties.getDbRollbackTableName();
        var changeLogTableName = properties.getDbChangeLogTable();
        var db = liquibase.getDatabase();
        var executor = getExecutor(db);

        try {
            liquibase.listUnexpectedChangeSets(new Contexts(), new LabelExpression())
                    .stream()
                    .sorted(Comparator.comparingInt(RanChangeSet::getOrderExecuted).reversed())
                    .forEach(changeSet -> {
                        log.info("Unexpected changeset found - {}", changeSet);
                        if (changeSet.getLastCheckSum() == null) {
                            log.info("There is no checksum for {} - skipping rollback", changeSet.getId());
                        }

                        try {
                            var rows = executor.queryForList(new SelectStatement(db.getLiquibaseCatalogName(),
                                    db.getLiquibaseSchemaName(), tableName)
                                    .addColumnsToSelect(COL_ROLLBACKSTMT)
                                    .setWhere(":name = :value AND :name = :value")
                                    .addWhereColumnNames(COL_CHANGELOG_ID, COL_CHANGELOG_CHECKSUM)
                                    .addWhereParameters(changeSet.getId(), changeSet.getLastCheckSum().toString())
                                    .setOrderBy(COL_ROLLBACKSTMTORDER));
                            if (!rows.isEmpty()) {
                                rows.forEach(row -> {
                                    log.info("Executing rollback statement for {}", changeSet.getId());
                                    try {
                                        executor.execute(new RawParameterizedSqlStatement(
                                                row.get(COL_ROLLBACKSTMT).toString()));
                                    } catch (DatabaseException e) {
                                        throw new RuntimeException(e);
                                    }
                                });

                                log.info("Deleting changeset {} record from changelog table", changeSet.getId());
                                logUpdatedRecords("deleted", executor.update(new DeleteStatement(
                                        db.getLiquibaseCatalogName(), db.getLiquibaseSchemaName(), changeLogTableName)
                                        .setWhere(":name = :value AND :name = :value")
                                        .addWhereColumnName(COL_DBCHANGELOG_ID)
                                        .addWhereColumnName(COL_DBCHANGELOG_MD5SUM)
                                        .addWhereParameters(changeSet.getId(), changeSet.getLastCheckSum().toString())));

                                log.info("Deleting rolled back statements for the changeset {}", changeSet.getId());
                                logUpdatedRecords("deleted", executor.update(new DeleteStatement(
                                        db.getLiquibaseCatalogName(), db.getLiquibaseSchemaName(), tableName)
                                        .setWhere(":name = :value AND :name = :value")
                                        .addWhereColumnName(COL_CHANGELOG_ID)
                                        .addWhereColumnName(COL_CHANGELOG_CHECKSUM)
                                        .addWhereParameters(changeSet.getId(), changeSet.getLastCheckSum().toString())));

                                db.commit();
                            } else {
                                log.info("There is no rollback statement for changeset {} ({})",
                                        changeSet.getId(), changeSet.getLastCheckSum().toString());
                            }
                        } catch (DatabaseException e) {
                            throw new UnexpectedLiquibaseException("Unable to rollback changesets - " + e.getMessage(), e);
                        }
                    });
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException("Unable to list unexpected changesets - " + e.getMessage(), e);
        }
    }

    protected void persistRollbackStatements(Liquibase liquibase) {
        var tableName = properties.getDbRollbackTableName();
        var db = liquibase.getDatabase();
        var sqlGenerator = SqlGeneratorFactory.getInstance();

        try {
            liquibase.listUnrunChangeSets(new Contexts(), new LabelExpression())
                    .forEach(changeSet -> {
                        log.info("Processing unrun changeset {}", changeSet.getId());
                        var checksum = changeSet.generateCheckSum(ChecksumVersion.latest()).toString();

                        if (changeSet.hasCustomRollbackChanges()) {
                            changeSet.getRollback().getChanges().forEach(rollbackChange ->
                                insertRollbackStatements(db, tableName, changeSet.getId(), checksum,
                                        sqlGenerator.generateSql(rollbackChange, db)));
                        } else if (!changeSet.getFilePath().toLowerCase().endsWith(".sql")) {
                            var changes = new ArrayList<>(changeSet.getChanges());
                            Collections.reverse(changes);
                            var sqlList = changes.stream()
                                    .filter(not(RawSQLChange.class::isInstance))
                                    .flatMap(change -> {
                                        try {
                                            return Arrays.stream(sqlGenerator.generateSql(
                                                    change.generateRollbackStatements(db), db));
                                        } catch (RollbackImpossibleException e) {
                                            log.warn("Unable to generate a rollback statement for the changeset {}",
                                                    changeSet.getId());
                                            return Stream.empty();
                                        }
                                    })
                                    .toList();
                            insertRollbackStatements(db, tableName, changeSet.getId(), checksum,
                                    sqlList.toArray(Sql[]::new));
                        }
                    });
            db.commit();
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException("Unable to persist rollback statements - " + e.getMessage(), e);
        }
    }

    private Executor getExecutor(Database db) {
        return Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor("jdbc", db);
    }

    private void logUpdatedRecords(String action, int count) {
        log.info("{} {} {}", count, (count == 1 ? "record" : "records"), action);
    }

    private void insertRollbackStatements(Database db, String tableName,
            String changeSetId, String changeSetCheckSum, Sql[] statements) {

        var executor = getExecutor(db);
        var stmtOrder = 1;
        for (var sql : statements) {
            try {
                logUpdatedRecords("inserted", executor.update(new InsertStatement(
                        db.getLiquibaseCatalogName(), db.getLiquibaseSchemaName(), tableName)
                        .addColumnValue(COL_CHANGELOG_ID, changeSetId)
                        .addColumnValue(COL_CHANGELOG_CHECKSUM, changeSetCheckSum)
                        .addColumnValue(COL_ROLLBACKSTMT, sql.toSql())
                        .addColumnValue(COL_ROLLBACKSTMTORDER, stmtOrder++)));
            } catch (DatabaseException e) {
                throw new UnexpectedLiquibaseException(
                        "Unable to insert a rollback record - " + e.getMessage(), e);
            }
        }
    }
}
