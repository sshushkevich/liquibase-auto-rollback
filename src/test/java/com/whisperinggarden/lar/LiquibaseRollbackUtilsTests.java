package com.whisperinggarden.lar;

import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LiquibaseRollbackUtilsTests {

    private static Connection connection;
    private static Database database;

    @BeforeEach
    public void beforeEach() throws Exception {
        connection = DriverManager.getConnection("jdbc:h2:mem:testdb" + UUID.randomUUID(), "sa", "");
        database = DatabaseFactory.getInstance()
                .findCorrectDatabaseImplementation(new JdbcConnection(connection));
        database.setLiquibaseCatalogName("PUBLIC");
        database.setLiquibaseSchemaName("PUBLIC");
    }

    @AfterEach
    public void afterEach() throws Exception {
        connection.close();
    }

    @Test
    public void whenTableDoesNotExist_thenRollbackTableAndIndexAreCreated() throws Exception {
        LiquibaseRollbackUtils.createRollbackTable(database, "ROLLBACK_TEST_TBL");

        try (var stmt = connection.createStatement()) {
            var tableRs = stmt.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_NAME = 'ROLLBACK_TEST_TBL'");
            assertThat(tableRs.next()).isTrue();

            var indexRs = stmt.executeQuery("SELECT * FROM INFORMATION_SCHEMA.INDEXES " +
                    "WHERE TABLE_NAME = 'ROLLBACK_TEST_TBL' AND INDEX_NAME = 'IDX_RB_CHANGELOGID'");
            assertThat(indexRs.next()).isTrue();

            var columnsRs = stmt.executeQuery("""
                            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_NAME = 'ROLLBACK_TEST_TBL'
                                ORDER BY ORDINAL_POSITION
                    """);

            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("ID");
            assertThat(columnsRs.getString("DATA_TYPE")).isEqualTo("INTEGER");
            assertThat(columnsRs.getString("IS_NULLABLE")).isEqualTo("NO");

            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("CHANGELOGID");
            assertThat(columnsRs.getString("DATA_TYPE")).isEqualTo("CHARACTER VARYING");
            assertThat(columnsRs.getString("IS_NULLABLE")).isEqualTo("NO");

            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("MD5SUM");
            assertThat(columnsRs.getString("DATA_TYPE")).isEqualTo("CHARACTER VARYING");
            assertThat(columnsRs.getString("IS_NULLABLE")).isEqualTo("NO");

            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("ROLLBACKSTMT");
            assertThat(columnsRs.getString("DATA_TYPE")).isEqualTo("CHARACTER VARYING");
            assertThat(columnsRs.getString("IS_NULLABLE")).isEqualTo("NO");

            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("ROLLBACKSTMTORDER");
            assertThat(columnsRs.getString("DATA_TYPE")).isEqualTo("INTEGER");
            assertThat(columnsRs.getString("IS_NULLABLE")).isEqualTo("NO");

            assertThat(columnsRs.next()).isFalse();
        }
    }

    @Test
    public void whenTableAlreadyExists_thenRollbackTableIsNotRecreatedOrDuplicated() throws Exception {
        LiquibaseRollbackUtils.createRollbackTable(database, "ROLLBACK_TEST_TBL");
        LiquibaseRollbackUtils.createRollbackTable(database, "ROLLBACK_TEST_TBL");

        try (var stmt = connection.createStatement()) {
            var countRs = stmt.executeQuery("SELECT COUNT(*) FROM ROLLBACK_TEST_TBL");
            assertThat(countRs.next()).isTrue();
        }
    }
}
