package com.whisperinggarden.lar;

import liquibase.Scope;
import liquibase.change.AddColumnConfig;
import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.AutoIncrementConstraint;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.core.CreateIndexStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.structure.core.Column;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LiquibaseRollbackUtils {

    public static void createRollbackTable(Database database, String tableName) throws DatabaseException {
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
                new AddColumnConfig(new Column("CHANGELOGID(15)"))));
    }
}
