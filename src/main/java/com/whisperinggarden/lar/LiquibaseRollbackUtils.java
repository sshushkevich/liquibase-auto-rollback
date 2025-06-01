/*
 * Copyright (c) 2025 Sergei Shushkevich
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package com.whisperinggarden.lar;

import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.snapshot.InvalidExampleException;
import liquibase.snapshot.SnapshotGeneratorFactory;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LiquibaseRollbackUtils {

    public static boolean hasTable(Database database, String tableName) {
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
