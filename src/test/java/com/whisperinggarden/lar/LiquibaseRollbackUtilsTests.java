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

import static com.whisperinggarden.lar.LiquibaseRollbackUtils.COL_CHANGELOGID;
import static com.whisperinggarden.lar.LiquibaseRollbackUtils.COL_ID;
import static com.whisperinggarden.lar.LiquibaseRollbackUtils.COL_MD5SUM;
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
                    %s VARCHAR(35) NOT NULL,
                    %s VARCHAR(4096) NOT NULL,
                    %s INT NOT NULL
                );
            """.formatted(ROLLBACK_TBL, COL_ID, COL_CHANGELOGID, COL_MD5SUM, COL_ROLLBACKSTMT, COL_ROLLBACKSTMTORDER));
        }

        LiquibaseRollbackUtils.persistRollbackStatements(createLiquibase(), ROLLBACK_TBL);

        try (var stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM " + ROLLBACK_TBL);
            rs.next();
            int count = rs.getInt(1);

            assertThat(count).isEqualTo(1);

            rs = stmt.executeQuery("SELECT %s, %s FROM %s".formatted(COL_CHANGELOGID, COL_ROLLBACKSTMT, ROLLBACK_TBL));
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("ID-01");
            assertThat(rs.getString(2)).isEqualTo("DROP TABLE PUBLIC.person");
        }
    }

    @Test
    public void whenUnexpectedChangesetsExist_thenRollbackStatementsAreExecuted() throws SQLException, LiquibaseException {
        var liquibase = createLiquibase();
        liquibase.update();
        var changeSetId = "ID-123";
        var changeSetChecksum = "9:76d0caf518233544632705e958b00fd2";
        var rollbackStmt = "DELETE FROM book WHERE title = 'Book 2'";
        try (var stmt = connection.createStatement()) {
            stmt.executeUpdate("""
                    INSERT INTO %s (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, EXECTYPE, MD5SUM)
                    VALUES ('%s', 'author', 'changes-1.sql', NOW(), 1, 'EXECUTED', '%s')
                    """.formatted(CHANGELOG_TBL, changeSetId, changeSetChecksum));
            stmt.executeUpdate("""
                    CREATE TABLE %s (
                        %s INT AUTO_INCREMENT PRIMARY KEY,
                        %s VARCHAR(255) NOT NULL,
                        %s VARCHAR(35) NOT NULL,
                        %s VARCHAR(4096) NOT NULL,
                        %s INT NOT NULL
                    )
                    """.formatted(ROLLBACK_TBL, COL_ID, COL_CHANGELOGID, COL_MD5SUM,
                    COL_ROLLBACKSTMT, COL_ROLLBACKSTMTORDER));
            stmt.executeUpdate("""
                    CREATE TABLE book (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        title VARCHAR(255) NOT NULL
                    )
                    """);
            stmt.executeUpdate("""
                    INSERT INTO book (title)
                    VALUES ('Book 1'), ('Book 2'), ('Book 3')
                    """);
        }
        try (var stmt = connection.prepareStatement("INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)"
                .formatted(ROLLBACK_TBL, COL_CHANGELOGID, COL_MD5SUM, COL_ROLLBACKSTMT, COL_ROLLBACKSTMTORDER))) {
            stmt.setString(1, changeSetId);
            stmt.setString(2, changeSetChecksum);
            stmt.setString(3, rollbackStmt);
            stmt.setInt(4, 1);
            stmt.executeUpdate();
        }
        connection.commit();

        LiquibaseRollbackUtils.rollbackUnexpectedChangeSets(liquibase, ROLLBACK_TBL, CHANGELOG_TBL);

        try (var stmt = connection.createStatement()) {
            var rs = stmt.executeQuery("SELECT COUNT(*) FROM book");
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(2);

            rs = stmt.executeQuery("SELECT title FROM book ORDER BY title");
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("Book 1");
            rs.next();
            assertThat(rs.getString(1)).isEqualTo("Book 3");

            rs = stmt.executeQuery("SELECT COUNT(*) FROM %s WHERE %s = '%s'"
                    .formatted(ROLLBACK_TBL, COL_CHANGELOGID, changeSetId));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(0);

            rs = stmt.executeQuery("SELECT COUNT(*) FROM %s WHERE ID = '%s'"
                    .formatted(CHANGELOG_TBL, changeSetId));
            rs.next();
            assertThat(rs.getInt(1)).isEqualTo(0);
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
