package io.github.salbifaza.flink.clickhouse;

import org.apache.flink.connector.jdbc.core.database.JdbcFactory;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;

/**
 * Registers {@link ClickHouseDialect} for {@code jdbc:clickhouse:} URLs. Discovered through
 * {@code META-INF/services/org.apache.flink.connector.jdbc.core.database.JdbcFactory}.
 */
public class ClickHouseFactory implements JdbcFactory {

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:clickhouse:") || url.startsWith("jdbc:ch:");
    }

    @Override
    public JdbcDialect createDialect() {
        return new ClickHouseDialect();
    }

    @Override
    public JdbcCatalog createCatalog(
            ClassLoader classLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String pwd,
            String baseUrl) {
        throw new UnsupportedOperationException(
                "The ClickHouse dialect supports JDBC sink tables only, not a JDBC catalog.");
    }
}
