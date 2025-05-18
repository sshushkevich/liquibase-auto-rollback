package com.whisperinggarden.lar;

import liquibase.database.core.H2Database;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;

public class SelectStatementTests {

    @Test
    void whenSelectStatementIsCreated_thenSqlGeneratorSupportsIt() {
        assertThat(SqlGeneratorFactory.getInstance().supports(new SelectStatement(null, null, null), new H2Database()))
                .isTrue();
    }

    @Test
    void whenConstructed_thenStoresCatalogSchemaAndTableNames() {
        var stmt = new SelectStatement("cat", "sch", "tbl");

        assertThat(stmt.getCatalogName()).isEqualTo("cat");
        assertThat(stmt.getSchemaName()).isEqualTo("sch");
        assertThat(stmt.getTableName()).isEqualTo("tbl");
    }

    @Test
    void whenUsingMethods_thenCanChainCalls() {
        var stmt = new SelectStatement("cat", "sch", "tbl")
                .addColumnsToSelect("col1")
                .setWhere(":name = :value")
                .addWhereColumnNames("col2")
                .addWhereParameters(123)
                .setOrderBy("col3");

        assertThat(stmt.getTableName()).isEqualTo("tbl");
        assertThat(stmt.getColumnsToSelect()).asInstanceOf(LIST).containsExactly("col1");
        assertThat(stmt.getWhere()).isEqualTo(":name = :value");
        assertThat(stmt.getWhereColumnNames()).asInstanceOf(LIST).containsExactly("col2");
        assertThat(stmt.getWhereParameters()).asInstanceOf(LIST).containsExactly(123);
        assertThat(stmt.getOrderBy()).isEqualTo("col3");
    }
}
