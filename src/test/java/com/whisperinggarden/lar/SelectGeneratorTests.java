package com.whisperinggarden.lar;

import liquibase.database.Database;
import liquibase.database.core.H2Database;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class SelectGeneratorTests {

    private final SelectGenerator generator = new SelectGenerator();
    private final Database database = new H2Database();

    @Test
    void whenTableNameIsMissing_thenValidationFails() {
        var stmt = new SelectStatement(null, null, null);

        var errors = generator.validate(stmt, database, null);

        assertThat(errors.hasErrors()).isTrue();
        assertThat(errors.getErrorMessages().contains("tableName is required")).isTrue();
        assertThat(errors.getErrorMessages().contains("No columnsToSelect defined")).isTrue();
    }

    @Test
    void whenWhereIsNullAndWhereFieldsAndParamsAreSet_thenValidationFails() {
        var stmt = new SelectStatement(null, null, "tbl")
                .addColumnsToSelect("col1")
                .addWhereColumnNames("col2")
                .addWhereParameters("param");

        var errors = generator.validate(stmt, database, null);

        assertThat(errors.hasErrors()).isTrue();
        assertThat(errors.getErrorMessages().contains("whereColumnNames set but no where clause")).isTrue();
        assertThat(errors.getErrorMessages().contains("whereParameters set but no where clause")).isTrue();
    }

    @Test
    void whenAllFieldsAreValid_thenValidationSucceeds() {
        var stmt = new SelectStatement(null, null, "tbl")
                .addColumnsToSelect("col1")
                .setWhere(":name = :value")
                .addWhereColumnNames("col2")
                .addWhereParameters("param");

        var errors = generator.validate(stmt, database, null);

        assertThat(errors.hasErrors()).isFalse();
    }

    @Test
    public void whenSelectStatementWithParams_thenGeneratesCorrectSql() {
        var statement = new SelectStatement(null, "PUBLIC", "DBRB")
                .addColumnsToSelect("col1", "col2", "col3")
                .setWhere(":name = :value AND :name = :value")
                .addWhereColumnNames("col2", "col3")
                .addWhereParameters("test value", 123)
                .setOrderBy("stmt_order");


        var result = generator.generateSql(statement, database, null);

        assertThat(result).hasSize(1);
        assertThat(result[0].toSql()).isEqualTo(
                "SELECT col1, col2, col3 FROM PUBLIC.DBRB " +
                "WHERE col2 = 'test value' AND col3 = 123 " +
                "ORDER BY stmt_order"
        );
    }
}
