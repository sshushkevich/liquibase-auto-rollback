package com.whisperinggarden.lar;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

import static com.whisperinggarden.lar.LiquibaseRollbackUtils.COL_CHANGELOG_CHECKSUM;
import static com.whisperinggarden.lar.LiquibaseRollbackUtils.COL_CHANGELOG_ID;
import static com.whisperinggarden.lar.LiquibaseRollbackUtils.COL_ID;
import static com.whisperinggarden.lar.LiquibaseRollbackUtils.COL_ROLLBACKSTMT;
import static com.whisperinggarden.lar.LiquibaseRollbackUtils.COL_ROLLBACKSTMTORDER;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LiquibaseRollbackUtilsTests {

    private static final String ROLLBACK_TBL = "ROLLBACK_TEST_TBL";
    private static final String CHANGELOG_TBL = "DATABASECHANGELOG";

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
        LiquibaseRollbackUtils.createRollbackTable(database, ROLLBACK_TBL);

        try (var stmt = connection.createStatement()) {
            var tableRs = stmt.executeQuery("SELECT * FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_NAME = '%s'".formatted(ROLLBACK_TBL));
            assertThat(tableRs.next()).isTrue();

            var indexRs = stmt.executeQuery("SELECT * FROM INFORMATION_SCHEMA.INDEXES " +
                    "WHERE TABLE_NAME = '%s' AND INDEX_NAME = 'IDX_RB_CHANGELOGID'".formatted(ROLLBACK_TBL));
            assertThat(indexRs.next()).isTrue();

            var columnsRs = stmt.executeQuery("""
                            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
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

            assertThat(columnsRs.next()).isTrue();
            assertThat(columnsRs.getString("COLUMN_NAME")).isEqualTo("CHANGELOGCHKSUM");
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
        LiquibaseRollbackUtils.createRollbackTable(database, ROLLBACK_TBL);
        LiquibaseRollbackUtils.createRollbackTable(database, ROLLBACK_TBL);

        try (var stmt = connection.createStatement()) {
            var countRs = stmt.executeQuery("SELECT COUNT(*) FROM " + ROLLBACK_TBL);
            assertThat(countRs.next()).isTrue();
        }
    }

    @Test
    public void whenUnrunChangesetExists_thenInsertRollbackRecord() throws SQLException {
        try (var stmt = connection.createStatement()) {
            stmt.executeUpdate("""
                CREATE TABLE %s (
                    %s INT AUTO_INCREMENT PRIMARY KEY,
                    %s VARCHAR(255) NOT NULL,
                    %s VARCHAR(100) NOT NULL,
                    %s VARCHAR(4096) NOT NULL,
                    %s INT NOT NULL
                );
            """.formatted(ROLLBACK_TBL, COL_ID, COL_CHANGELOG_ID, COL_CHANGELOG_CHECKSUM, COL_ROLLBACKSTMT, COL_ROLLBACKSTMTORDER));
        }

        LiquibaseRollbackUtils.persistRollbackStatements(createLiquibase(), ROLLBACK_TBL);

        try (var stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM " + ROLLBACK_TBL);
            rs.next();
            int count = rs.getInt(1);

            assertThat(count).isEqualTo(1);

            rs = stmt.executeQuery("SELECT %s, %s FROM %s".formatted(COL_CHANGELOG_ID, COL_ROLLBACKSTMT, ROLLBACK_TBL));
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("ID-01");
            assertThat(rs.getString(2)).isEqualTo("DROP TABLE PUBLIC.person");
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
        var rollbackStmt21 = "ALTER TABLE person RENAME COLUMN full_name TO name";
        var rollbackStmt22 = "DELETE FROM person WHERE name = 'John Doe'";
        try (var stmt = connection.createStatement()) {
            stmt.executeUpdate("""
                    INSERT INTO %s (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, EXECTYPE, MD5SUM)
                    VALUES ('%s', 'author', 'db/changelog/test-changelog-2.yaml', NOW(), 2, 'EXECUTED', '%s')
                    """.formatted(CHANGELOG_TBL, changeSetId2, changeSetChecksum2));
            stmt.executeUpdate("""
                    CREATE TABLE %s (
                        %s INT AUTO_INCREMENT PRIMARY KEY,
                        %s VARCHAR(255) NOT NULL,
                        %s VARCHAR(35) NOT NULL,
                        %s VARCHAR(4096) NOT NULL,
                        %s INT NOT NULL
                    )
                    """.formatted(ROLLBACK_TBL, COL_ID, COL_CHANGELOG_ID, COL_CHANGELOG_CHECKSUM,
                    COL_ROLLBACKSTMT, COL_ROLLBACKSTMTORDER));
            stmt.executeUpdate("""
                    INSERT INTO person (name)
                    VALUES ('Jane Doe'), ('John Doe'), ('Paul Smith')
                    """);
            stmt.executeUpdate("ALTER TABLE person RENAME COLUMN name TO full_name");
        }
        try (var stmt = connection.prepareStatement("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)"
                .formatted(ROLLBACK_TBL, COL_CHANGELOG_ID, COL_CHANGELOG_CHECKSUM, COL_ROLLBACKSTMT, COL_ROLLBACKSTMTORDER))) {
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

        LiquibaseRollbackUtils.rollbackUnexpectedChangeSets(liquibase, ROLLBACK_TBL, CHANGELOG_TBL);

        try (var stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM person");
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(2);

            rs = stmt.executeQuery("SELECT name FROM person ORDER BY name");
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("Jane Doe");
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("Paul Smith");

            rs = stmt.executeQuery("SELECT COUNT(*) FROM %s WHERE %s = '%s'"
                    .formatted(ROLLBACK_TBL, COL_CHANGELOG_ID, changeSetId1));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);

            rs = stmt.executeQuery("SELECT COUNT(*) FROM %s WHERE %s = '%s'"
                    .formatted(ROLLBACK_TBL, COL_CHANGELOG_ID, changeSetId2));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(0);

            rs = stmt.executeQuery("SELECT COUNT(*) FROM %s WHERE ID = '%s'"
                    .formatted(CHANGELOG_TBL, changeSetId1));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(1);

            rs = stmt.executeQuery("SELECT COUNT(*) FROM %s WHERE ID = '%s'"
                    .formatted(CHANGELOG_TBL, changeSetId2));
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
        return new Liquibase(
                "db/changelog/test-changelog.yaml",
                new ClassLoaderResourceAccessor(),
                database
        );
    }
}
