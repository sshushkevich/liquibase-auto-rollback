package com.whisperinggarden.lar;

import liquibase.ChecksumVersion;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.Scope;
import liquibase.change.AddColumnConfig;
import liquibase.changelog.RanChangeSet;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.AutoIncrementConstraint;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateIndexStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.statement.core.DeleteStatement;
import liquibase.statement.core.InsertStatement;
import liquibase.statement.core.RawParameterizedSqlStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;

@Slf4j
public class LiquibaseRollbackUtils {

    // DATABASECHANGELOGRB table columns:
    public static final String COL_ID = "ID";
    public static final String COL_CHANGELOG_ID = "CHANGELOGID";
    public static final String COL_CHANGELOG_CHECKSUM = "CHANGELOGCHKSUM";
    public static final String COL_ROLLBACKSTMT = "ROLLBACKSTMT";
    public static final String COL_ROLLBACKSTMTORDER = "ROLLBACKSTMTORDER";

    // DATABASECHANGELOG table columns:
    public static final String COL_DBCHANGELOG_ID = "ID";
    public static final String COL_DBCHANGELOG_MD5SUM = "MD5SUM";

    public static void createRollbackTable(Database db, String tableName, int rollbackStmtMaxLength) {
        if (hasTable(db, tableName)) {
            return;
        }

        var executor = Scope.getCurrentScope()
                .getSingleton(ExecutorService.class)
                .getExecutor("jdbc", db);

        var intType = DataTypeFactory.getInstance().fromDescription("int", db);
        var varchar255Type = DataTypeFactory.getInstance().fromDescription("varchar(255)", db);
        var varchar100Type = DataTypeFactory.getInstance().fromDescription("varchar(100)", db);
        var varchar4KType = DataTypeFactory.getInstance()
                .fromDescription("varchar(%d)".formatted(rollbackStmtMaxLength), db);

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

    public static void persistRollbackStatements(Liquibase liquibase, String tableName) {
        var db = liquibase.getDatabase();
        var sqlGenerator = SqlGeneratorFactory.getInstance();
        var executor = Scope.getCurrentScope()
                .getSingleton(ExecutorService.class)
                .getExecutor("jdbc", liquibase.getDatabase());

        try {
            liquibase.listUnrunChangeSets(new Contexts(), new LabelExpression())
                    .forEach(changeSet -> {
                        log.info("Processing unrun changeset {}", changeSet.getId());

                        var checksum = changeSet.generateCheckSum(ChecksumVersion.latest()).toString();
                        changeSet.getRollback().getChanges().forEach(rollbackChange -> {
                            var stmtOrder = 1;
                            for (var sql : sqlGenerator.generateSql(rollbackChange, db)) {
                                try {
                                    logUpdatedRecords("inserted", executor.update(new InsertStatement(
                                                    db.getLiquibaseCatalogName(), db.getLiquibaseSchemaName(), tableName)
                                            .addColumnValue(COL_CHANGELOG_ID, changeSet.getId())
                                            .addColumnValue(COL_CHANGELOG_CHECKSUM, checksum)
                                            .addColumnValue(COL_ROLLBACKSTMT, sql.toSql())
                                            .addColumnValue(COL_ROLLBACKSTMTORDER, stmtOrder++)));
                                } catch (DatabaseException e) {
                                    throw new UnexpectedLiquibaseException(
                                            "Unable to insert rollback record - " + e.getMessage(), e);
                                }
                            }
                        });
                    });
            db.commit();
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException("Unable to persist rollback statements - " + e.getMessage(), e);
        }
    }

    public static void rollbackUnexpectedChangeSets(Liquibase liquibase, String tableName, String changeLogTableName) {
        var db = liquibase.getDatabase();
        var executor = Scope.getCurrentScope()
                .getSingleton(ExecutorService.class)
                .getExecutor("jdbc", liquibase.getDatabase());

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

    private static boolean hasTable(Database database, String tableName) {
        try {
            return SnapshotGeneratorFactory.getInstance().has(
                    new Table().setName(tableName).setSchema(
                            new Schema(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName())),
                    database);
        } catch (InvalidExampleException | DatabaseException e) {
            throw new UnexpectedLiquibaseException("Unable to verify whether the rollback table exists", e);
        }
    }

    private static void logUpdatedRecords(String action, int count) {
        log.info("{} {} {}", count, (count == 1 ? "record" : "records"), action);
    }
}
