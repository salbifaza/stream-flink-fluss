package io.github.salbifaza.flink.clickhouse;

import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

/**
 * JDBC dialect for ClickHouse, append-only by design.
 *
 * <p>ClickHouse has no row-level UPSERT, and its DELETE is a heavyweight mutation, so this
 * dialect deliberately offers no upsert statement. The pipeline never needs one: every sink
 * reads a Fluss {@code $changelog} table, which is insert-only, and writes each change as a new
 * row into a {@code ReplacingMergeTree(_version, is_deleted)} table. ClickHouse then keeps the
 * highest {@code _version} per key and hides rows flagged {@code is_deleted}.
 *
 * <p>A sink table declared with a PRIMARY KEY would make Flink ask for an upsert statement and
 * fail at planning time, which is the intended guard against using it the wrong way.
 */
public class ClickHouseDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    @Override
    public String dialectName() {
        return "ClickHouse";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.clickhouse.jdbc.Driver");
    }

    @Override
    public JdbcDialectConverter getRowConverter(RowType rowType) {
        return new ClickHouseDialectConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(1, 76));
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(0, 9));
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
    }
}
