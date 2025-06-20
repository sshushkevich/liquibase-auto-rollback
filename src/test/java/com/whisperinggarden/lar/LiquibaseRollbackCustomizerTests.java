/*
 * Copyright (c) 2025 Sergei Shushkevich
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package com.whisperinggarden.lar;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

import static com.whisperinggarden.lar.LiquibaseRollbackCustomizer.COL_CHANGELOG_CHECKSUM;
import static com.whisperinggarden.lar.LiquibaseRollbackCustomizer.COL_CHANGELOG_ID;
import static com.whisperinggarden.lar.LiquibaseRollbackCustomizer.COL_ID;
import static com.whisperinggarden.lar.LiquibaseRollbackCustomizer.COL_ROLLBACKSTMT;
import static com.whisperinggarden.lar.LiquibaseRollbackCustomizer.COL_ROLLBACKSTMTORDER;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LiquibaseRollbackCustomizerTests {

    private static final String ROLLBACK_TBL = "ROLLBACK_TEST_TBL";
    private static final String CHANGELOG_TBL = "DATABASECHANGELOG";

    private static final String APP_SCHEMA = "APP_SCHEMA";
    private static final String LIQUBASE_SCHEMA = "LIQ_SCHEMA";

    private static LiquibaseRollbackCustomizer customizer;

    private Connection connection;
    private Database database;

    @BeforeAll
    public static void beforeAll() {
        var props = new LiquibaseRollbackProperties();
        props.setDbRollbackTableName(ROLLBACK_TBL);
        props.setDbChangeLogTable(CHANGELOG_TBL);
        props.setRollbackStatementMaxLength(1024);
        customizer = new LiquibaseRollbackCustomizer(props);
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        connection = DriverManager.getConnection("jdbc:h2:mem:testdb" + UUID.randomUUID(), "sa", "");
        createSchema(APP_SCHEMA);
        createSchema(LIQUBASE_SCHEMA);
        database = DatabaseFactory.getInstance()
                .findCorrectDatabaseImplementation(new JdbcConnection(connection));
        database.setDefaultSchemaName(APP_SCHEMA);
        database.setLiquibaseSchemaName(LIQUBASE_SCHEMA);
    }

    @AfterEach
    public void afterEach() throws Exception {
        connection.close();
    }

    @Test
    public void whenCustomizeIsCalled_ThenRollbackTableIsCreatedAndStmtInserted() throws SQLException {
        customizer.customize(createLiquibase());

        try (var stmt = connection.createStatement()) {
            var tableRs = stmt.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_NAME = '%s'".formatted(ROLLBACK_TBL));
            assertThat(tableRs.next()).isTrue();

            var rs = stmt.executeQuery("SELECT COUNT(*) FROM %s.%s".formatted(LIQUBASE_SCHEMA, ROLLBACK_TBL));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    public void whenCustomizeIsCalled_thenRollbackStatementsAreExecuted() throws SQLException, LiquibaseException {
        var liquibase = createLiquibase();
        liquibase.update();
        var changeSetId2 = "ID-02";
        var changeSetChecksum2 = "9:76d0caf518233544632705e958b00fd4";
        var rollbackStmt2 = "DROP TABLE %s.book".formatted(APP_SCHEMA);
        try (var stmt = connection.createStatement()) {
            stmt.executeUpdate("""
                CREATE TABLE %s.%s (
                    ID INT AUTO_INCREMENT PRIMARY KEY
                );
            """.formatted(APP_SCHEMA, "book"));
            stmt.executeUpdate("""
                    INSERT INTO %s.%s (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, EXECTYPE, MD5SUM)
                    VALUES ('%s', 'author', 'db/changelog/test-changelog-2.yaml', NOW(), 2, 'EXECUTED', '%s')
                    """.formatted(LIQUBASE_SCHEMA, CHANGELOG_TBL, changeSetId2, changeSetChecksum2));
        }
        createRollbackTableForTest();
        try (var stmt = connection.prepareStatement("INSERT INTO %s.%s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)"
                .formatted(LIQUBASE_SCHEMA, ROLLBACK_TBL, COL_CHANGELOG_ID, COL_CHANGELOG_CHECKSUM,
                        COL_ROLLBACKSTMT, COL_ROLLBACKSTMTORDER))) {
            stmt.setString(1, changeSetId2);
            stmt.setString(2, changeSetChecksum2);
            stmt.setString(3, rollbackStmt2);
            stmt.setInt(4, 1);
            stmt.executeUpdate();
        }
        connection.commit();

        customizer.customize(liquibase);

        try (var stmt = connection.createStatement()) {
            var tableRs = stmt.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_NAME = 'book'");
            assertThat(tableRs.next()).isFalse();

            var rs = stmt.executeQuery("SELECT COUNT(*) FROM %s.%s WHERE %s = '%s'"
                    .formatted(LIQUBASE_SCHEMA, ROLLBACK_TBL, COL_CHANGELOG_ID, changeSetId2));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(0);
        }
    }

    @Test
    public void whenTableDoesNotExist_thenRollbackTableAndIndexAreCreated() throws Exception {
        customizer.createRollbackTable(database);

        try (var stmt = connection.createStatement()) {
            var tableRs = stmt.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_NAME = '%s'".formatted(ROLLBACK_TBL));
            assertThat(tableRs.next()).isTrue();

            var indexRs = stmt.executeQuery("""
                    SELECT COLUMN_NAME, IS_UNIQUE
                    FROM INFORMATION_SCHEMA.INDEX_COLUMNS
                    WHERE TABLE_NAME = '%s' AND INDEX_NAME = '%s'
                    ORDER BY ORDINAL_POSITION
                    """.formatted(ROLLBACK_TBL, "IDX_RB_CHANGELOGIDSUMORD"));
            assertThat(indexRs.next()).isTrue();
            assertThat(indexRs.getString("COLUMN_NAME")).isEqualTo(COL_CHANGELOG_ID);
            assertThat(indexRs.getBoolean("IS_UNIQUE")).isEqualTo(true);
            assertThat(indexRs.next()).isTrue();
            assertThat(indexRs.getString("COLUMN_NAME")).isEqualTo(COL_CHANGELOG_CHECKSUM);
            assertThat(indexRs.getBoolean("IS_UNIQUE")).isEqualTo(true);
            assertThat(indexRs.next()).isTrue();
            assertThat(indexRs.getString("COLUMN_NAME")).isEqualTo(COL_ROLLBACKSTMTORDER);
            assertThat(indexRs.getBoolean("IS_UNIQUE")).isEqualTo(true);

            var columnsRs = stmt.executeQuery("""
                            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
                                FROM INFORMATION_SCHEMA.COLUMNS
                                WHERE TABLE_NAME = '%s'
                                ORDER BY ORDINAL_POSITION
                    """.formatted(ROLLBACK_TBL));

            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("ID");
            assertThat(columnsRs.getString("DATA_TYPE")).isEqualTo("INTEGER");
            assertThat(columnsRs.getString("IS_NULLABLE")).isEqualTo("NO");

            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("CHANGELOGID");
            assertThat(columnsRs.getString("DATA_TYPE")).isEqualTo("CHARACTER VARYING");
            assertThat(columnsRs.getString("IS_NULLABLE")).isEqualTo("NO");
            assertThat(columnsRs.getString("CHARACTER_MAXIMUM_LENGTH")).isEqualTo("255");

            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("CHANGELOGCHKSUM");
            assertThat(columnsRs.getString("DATA_TYPE")).isEqualTo("CHARACTER VARYING");
            assertThat(columnsRs.getString("IS_NULLABLE")).isEqualTo("NO");
            assertThat(columnsRs.getString("CHARACTER_MAXIMUM_LENGTH")).isEqualTo("100");

            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("ROLLBACKSTMT");
            assertThat(columnsRs.getString("DATA_TYPE")).isEqualTo("CHARACTER VARYING");
            assertThat(columnsRs.getString("IS_NULLABLE")).isEqualTo("NO");
            assertThat(columnsRs.getString("CHARACTER_MAXIMUM_LENGTH")).isEqualTo("1024");

            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("ROLLBACKSTMTORDER");
            assertThat(columnsRs.getString("DATA_TYPE")).isEqualTo("INTEGER");
            assertThat(columnsRs.getString("IS_NULLABLE")).isEqualTo("NO");

            assertThat(columnsRs.next()).isFalse();
        }
    }

    @Test
    public void whenTableAlreadyExists_thenRollbackTableIsNotRecreatedOrDuplicated() throws Exception {
        customizer.createRollbackTable(database);
        customizer.createRollbackTable(database);

        try (var stmt = connection.createStatement()) {
            var countRs = stmt.executeQuery("SELECT COUNT(*) FROM %s.%s".formatted(LIQUBASE_SCHEMA, ROLLBACK_TBL));
            assertThat(countRs.next()).isTrue();
        }
    }

    @Test
    public void whenUnrunChangesetExists_thenInsertRollbackRecord() throws SQLException {
        createRollbackTableForTest();

        customizer.persistRollbackStatements(createLiquibase());

        try (var stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM %s.%s".formatted(LIQUBASE_SCHEMA, ROLLBACK_TBL));
            rs.next();
            int count = rs.getInt(1);

            assertThat(count).isEqualTo(1);

            rs = stmt.executeQuery("SELECT %s, %s FROM %s.%s"
                    .formatted(COL_CHANGELOG_ID, COL_ROLLBACKSTMT, LIQUBASE_SCHEMA, ROLLBACK_TBL));
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("ID-01");
            assertThat(rs.getString(2)).isEqualTo("DROP TABLE %s.person".formatted(APP_SCHEMA));
        }
    }

    @Test
    public void whenUnrunChangesetWithNoRollbackExists_thenGenerateAndInsertRollbackRecord() throws SQLException {
        createRollbackTableForTest();

        customizer.persistRollbackStatements(createLiquibase("db/changelog/test-changelog-norollback.yaml"));

        try (var stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM %s.%s".formatted(LIQUBASE_SCHEMA, ROLLBACK_TBL));

            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(3);

            rs = stmt.executeQuery("SELECT %s, %s, %s FROM %s.%s"
                    .formatted(COL_CHANGELOG_ID, COL_ROLLBACKSTMT, COL_ROLLBACKSTMTORDER, LIQUBASE_SCHEMA, ROLLBACK_TBL));

            rs.next();
            assertThat(rs.getString(1)).isEqualTo("ID-01");
            assertThat(rs.getString(2)).isEqualTo("DROP TABLE %s.book".formatted(APP_SCHEMA));
            assertThat(rs.getInt(3)).isEqualTo(1);

            rs.next();
            assertThat(rs.getString(1)).isEqualTo("ID-02");
            assertThat(rs.getString(2)).isEqualTo("ALTER TABLE %s.book ALTER COLUMN author_full_name RENAME TO author_name"
                    .formatted(APP_SCHEMA));
            assertThat(rs.getInt(3)).isEqualTo(1);

            rs.next();
            assertThat(rs.getString(1)).isEqualTo("ID-02");
            assertThat(rs.getString(2)).isEqualTo("ALTER TABLE %s.book DROP COLUMN author_name".formatted(APP_SCHEMA));
            assertThat(rs.getInt(3)).isEqualTo(2);
        }
    }

    @Test
    public void whenUnexpectedChangesetsExist_thenRollbackStatementsAreExecuted() throws SQLException, LiquibaseException {
        var liquibase = createLiquibase();
        liquibase.update();
        var changeSetId1 = "ID-01";
        var changeSetId2 = "ID-02";
        var changeSetChecksum1 = "9:c7e301964fcdbeb3bc6509e62a018976";
        var changeSetChecksum2 = "9:76d0caf518233544632705e958b00fd4";
        var rollbackStmt1 = "DROP TABLE person";
        var rollbackStmt21 = "ALTER TABLE %s.person RENAME COLUMN full_name TO name".formatted(APP_SCHEMA);
        var rollbackStmt22 = "DELETE FROM %s.person WHERE name = 'John Doe'".formatted(APP_SCHEMA);
        try (var stmt = connection.createStatement()) {
            stmt.executeUpdate("""
                    INSERT INTO %s.%s (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, EXECTYPE, MD5SUM)
                    VALUES ('%s', 'author', 'db/changelog/test-changelog-2.yaml', NOW(), 2, 'EXECUTED', '%s')
                    """.formatted(LIQUBASE_SCHEMA, CHANGELOG_TBL, changeSetId2, changeSetChecksum2));
            stmt.executeUpdate("""
                    INSERT INTO %s.person (name)
                    VALUES ('Jane Doe'), ('John Doe'), ('Paul Smith')
                    """.formatted(APP_SCHEMA));
            stmt.executeUpdate("ALTER TABLE %s.person RENAME COLUMN name TO full_name".formatted(APP_SCHEMA));
        }
        createRollbackTableForTest();
        try (var stmt = connection.prepareStatement("INSERT INTO %s.%s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)"
                .formatted(LIQUBASE_SCHEMA, ROLLBACK_TBL, COL_CHANGELOG_ID, COL_CHANGELOG_CHECKSUM,
                        COL_ROLLBACKSTMT, COL_ROLLBACKSTMTORDER))) {
            stmt.setString(1, changeSetId1);
            stmt.setString(2, changeSetChecksum1);
            stmt.setString(3, rollbackStmt1);
            stmt.setInt(4, 1);
            stmt.addBatch();

            stmt.setString(1, changeSetId2);
            stmt.setString(2, changeSetChecksum2);
            stmt.setString(3, rollbackStmt21);
            stmt.setInt(4, 1);
            stmt.addBatch();

            stmt.setString(1, changeSetId2);
            stmt.setString(2, changeSetChecksum2);
            stmt.setString(3, rollbackStmt22);
            stmt.setInt(4, 2);
            stmt.addBatch();

            stmt.executeBatch();
        }
        connection.commit();

        customizer.rollbackUnexpectedChangeSets(liquibase);

        try (var stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM %s.person".formatted(APP_SCHEMA));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(2);

            rs = stmt.executeQuery("SELECT name FROM %s.person ORDER BY name".formatted(APP_SCHEMA));
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("Jane Doe");
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("Paul Smith");

            rs = stmt.executeQuery("SELECT COUNT(*) FROM %s.%s WHERE %s = '%s'"
                    .formatted(LIQUBASE_SCHEMA, ROLLBACK_TBL, COL_CHANGELOG_ID, changeSetId1));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);

            rs = stmt.executeQuery("SELECT COUNT(*) FROM %s.%s WHERE %s = '%s'"
                    .formatted(LIQUBASE_SCHEMA, ROLLBACK_TBL, COL_CHANGELOG_ID, changeSetId2));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(0);

            rs = stmt.executeQuery("SELECT COUNT(*) FROM %s.%s WHERE ID = '%s'"
                    .formatted(LIQUBASE_SCHEMA, CHANGELOG_TBL, changeSetId1));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);

            rs = stmt.executeQuery("SELECT COUNT(*) FROM %s.%s WHERE ID = '%s'"
                    .formatted(LIQUBASE_SCHEMA, CHANGELOG_TBL, changeSetId2));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(0);

            var columnsRs = stmt.executeQuery("""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'PERSON'
                    ORDER BY ORDINAL_POSITION
            """);
            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("NAME");
        }
    }

    private Liquibase createLiquibase() {
        return createLiquibase("db/changelog/test-changelog.yaml");
    }

    private Liquibase createLiquibase(String changeLogFile) {
        return new Liquibase(
                changeLogFile,
                new ClassLoaderResourceAccessor(),
                database
        );
    }

    private void createSchema(String schemaName) throws SQLException {
        try (var stmt = connection.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS %s".formatted(schemaName));
        }
    }

    private void createRollbackTableForTest() throws SQLException {
        try (var stmt = connection.createStatement()) {
            stmt.executeUpdate("""
                CREATE TABLE %s.%s (
                    %s INT AUTO_INCREMENT PRIMARY KEY,
                    %s VARCHAR(255) NOT NULL,
                    %s VARCHAR(100) NOT NULL,
                    %s VARCHAR(4096) NOT NULL,
                    %s INT NOT NULL
                );
            """.formatted(LIQUBASE_SCHEMA, ROLLBACK_TBL, COL_ID, COL_CHANGELOG_ID, COL_CHANGELOG_CHECKSUM,
                    COL_ROLLBACKSTMT, COL_ROLLBACKSTMTORDER));
        }
    }
}
