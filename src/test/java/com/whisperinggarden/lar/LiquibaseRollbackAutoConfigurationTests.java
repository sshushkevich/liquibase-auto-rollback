/*
 * Copyright (c) 2025 Sergei Shushkevich
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

package com.whisperinggarden.lar;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

public class LiquibaseRollbackAutoConfigurationTests {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(LiquibaseRollbackAutoConfiguration.class));

    @Test
    void whenPropertyIsMissing_thenCustomizerBeanShouldBeCreated() {
        contextRunner.run(context ->
            assertThat(context).hasSingleBean(LiquibaseRollbackCustomizer.class)
        );
    }

    @Test
    void whenPropertyIsEnabled_thenCustomizerBeanShouldBeCreated() {
        contextRunner.withPropertyValues("spring.liquibase-rollback.enabled=true")
                .run(context ->
                    assertThat(context).hasSingleBean(LiquibaseRollbackCustomizer.class)
                );
    }

    @Test
    void whenPropertyIsDisabled_thenCustomizerBeanShouldNotBeCreated() {
        contextRunner.withPropertyValues("spring.liquibase-rollback.enabled=false")
                .run(context ->
                    assertThat(context).doesNotHaveBean(LiquibaseRollbackCustomizer.class)
                );
    }
}
