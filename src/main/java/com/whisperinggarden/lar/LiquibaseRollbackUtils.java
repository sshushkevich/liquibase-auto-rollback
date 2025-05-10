package com.whisperinggarden.lar;

import liquibase.Scope;
import liquibase.change.AddColumnConfig;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.statement.AutoIncrementConstraint;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateIndexStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.structure.core.Column;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LiquibaseRollbackUtils {

    public static void createRollbackTable(Database database, String tableName) throws DatabaseException {
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

        executor.execute(new CreateTableStatement(null, null, tableName)
                .addPrimaryKeyColumn("ID", intType, null, null, null, new AutoIncrementConstraint("ID"))
                .addColumn("CHANGELOGID", varchar255Type, new NotNullConstraint("CHANGELOGID"))
                .addColumn("MD5SUM", varchar35Type, new NotNullConstraint("MD5SUM"))
                .addColumn("ROLLBACKSTMT", varchar4KType, new NotNullConstraint("ROLLBACKSTMT"))
                .addColumn("ROLLBACKSTMTORDER", intType, new NotNullConstraint("ROLLBACKSTMTORDER")));

        executor.execute(new CreateIndexStatement("IDX_RB_CHANGELOGID", null, null, tableName, false, null,
                new AddColumnConfig(new Column("CHANGELOGID"))));
    }

    private static boolean hasTable(Database database, String tableName) throws DatabaseException {
        try {
            return SnapshotGeneratorFactory.getInstance().has(
                    new Table().setName(tableName).setSchema(
                            new Schema(database.getLiquibaseCatalogName(), database.getLiquibaseSchemaName())),
                    database);
        } catch (InvalidExampleException e) {
            throw new UnexpectedLiquibaseException("Unable to verify whether the rollback table exists", e);
        }
    }
}
