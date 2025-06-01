/*
 * Copyright (c) 2025 Sergei Shushkevich
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package com.whisperinggarden.lar;

import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class LiquibaseRollbackUtilsTests {

    private Connection connection;
    private Database database;

    @BeforeEach
    public void beforeEach() throws Exception {
        connection = DriverManager.getConnection("jdbc:h2:mem:testdb" + UUID.randomUUID(), "sa", "");
        database = DatabaseFactory.getInstance()
                .findCorrectDatabaseImplementation(new JdbcConnection(connection));
    }

    @AfterEach
    public void afterEach() throws Exception {
        connection.close();
    }

    @Test
    public void whenTableExists_ThenHasTableReturnsTrue() throws SQLException {
        try (var stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE test_table (ID INT AUTO_INCREMENT PRIMARY KEY)");
        }
        assertThat(LiquibaseRollbackUtils.hasTable(database, "test_table")).isTrue();
    }

    @Test
    public void whenTableDoesNotExist_ThenHasTableReturnsFalse() throws SQLException {
        assertThat(LiquibaseRollbackUtils.hasTable(database, "nonexistent_table")).isFalse();
    }
}
