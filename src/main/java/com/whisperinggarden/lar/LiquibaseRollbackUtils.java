package com.whisperinggarden.lar;

import liquibase.ChecksumVersion;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.Scope;
import liquibase.change.AddColumnConfig;
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
import liquibase.statement.core.InsertStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LiquibaseRollbackUtils {

    public static final String COL_ID = "ID";
    public static final String COL_CHANGELOGID = "CHANGELOGID";
    public static final String COL_MD5SUM = "MD5SUM";
    public static final String COL_ROLLBACKSTMT = "ROLLBACKSTMT";
    public static final String COL_ROLLBACKSTMTORDER = "ROLLBACKSTMTORDER";

    public static void createRollbackTable(Database database, String tableName) {
        if (hasTable(database, tableName)) {
            return;
        }

        var executor = Scope.getCurrentScope()
                .getSingleton(ExecutorService.class)
                .getExecutor("jdbc", database);

        var intType = DataTypeFactory.getInstance().fromDescription("int", database);
        var varchar255Type = DataTypeFactory.getInstance().fromDescription("varchar(255)", database);
        var varchar35Type = DataTypeFactory.getInstance().fromDescription("varchar(35)", database);
        var varchar4KType = DataTypeFactory.getInstance().fromDescription("varchar(4096)", database);

        log.info("Creating {} table", tableName);

        try {
            executor.execute(new CreateTableStatement(null, null, tableName)
                    .addPrimaryKeyColumn(COL_ID, intType, null, null, null, new AutoIncrementConstraint(COL_ID))
                    .addColumn(COL_CHANGELOGID, varchar255Type, new NotNullConstraint(COL_CHANGELOGID))
                    .addColumn(COL_MD5SUM, varchar35Type, new NotNullConstraint(COL_MD5SUM))
                    .addColumn(COL_ROLLBACKSTMT, varchar4KType, new NotNullConstraint(COL_ROLLBACKSTMT))
                    .addColumn(COL_ROLLBACKSTMTORDER, intType, new NotNullConstraint(COL_ROLLBACKSTMTORDER)));

            executor.execute(new CreateIndexStatement("IDX_RB_CHANGELOGID", null, null, tableName, false, null,
                    new AddColumnConfig(new Column(COL_CHANGELOGID))));
        } catch (DatabaseException e) {
            throw new UnexpectedLiquibaseException("Unable to create the rollback table", e);
        }
    }

    public static void persistRollbackStatements(Liquibase liquibase, String tableName) {
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
                            for (var sql : sqlGenerator.generateSql(rollbackChange, liquibase.getDatabase())) {
                                try {
                                    executor.execute(new InsertStatement(null, null, tableName)
                                            .addColumnValue(COL_CHANGELOGID, changeSet.getId())
                                            .addColumnValue(COL_MD5SUM, checksum)
                                            .addColumnValue(COL_ROLLBACKSTMT, sql.toSql())
                                            .addColumnValue(COL_ROLLBACKSTMTORDER, stmtOrder++));
                                } catch (DatabaseException e) {
                                    throw new UnexpectedLiquibaseException(
                                            "Unable to insert rollback record - " + e.getMessage(), e);
                                }

                            }
                        });
                    });
        } catch (LiquibaseException e) {
            throw new UnexpectedLiquibaseException("Unable to persist rollback statements - " + e.getMessage(), e);
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
}
