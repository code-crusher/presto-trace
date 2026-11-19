/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.execution.TestingPageSourceProvider;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorPageSourceProvider;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.UpdateNode;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingSplitManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.NATIVE_UPDATE_MERGE_ENABLED;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

/**
 * Verifies the planner-level gate that keeps the legacy {@code UpdateNode} pipeline off the
 * native (Prestissimo) engine. Native workers have no {@code UpdateNode} translator, so a
 * plan that ships one dies on the worker with an opaque protocol error
 * ({@code ...UpdateNode no abstract type PlanNode}) wrapped in a task-update HTTP 500.
 * The planner fails such queries fast on the coordinator instead. Two gates compose:
 * {@code LogicalPlanner#createUpdatePlan} rejects native sessions with
 * {@code native_update_merge_enabled} disabled, and
 * {@code QueryPlanner#checkUpdateNotShippedToNativeWorkers} rejects the remaining
 * native sessions whose target table cannot take the UPDATE-as-MERGE rewrite.
 */
public class TestNativeUpdatePlannerGate
        extends BasePlanTest
{
    private static final String CATALOG_NAME = "updatable";
    private static final String TABLE_NAME = "t_update_target";
    private static final String QUALIFIED_TABLE_NAME = CATALOG_NAME + ".default." + TABLE_NAME;
    private static final String UPDATE_SQL = "UPDATE " + QUALIFIED_TABLE_NAME + " SET name = 'b' WHERE id = 1";

    @BeforeClass
    public void createUpdatableCatalog()
    {
        getQueryRunner().createCatalog(CATALOG_NAME, new UpdatableConnectorFactory(), ImmutableMap.of());
    }

    private static Session nativeSession(Session base)
    {
        return nativeSession(base, true);
    }

    private static Session nativeSession(Session base, boolean updateMergeEnabled)
    {
        return Session.builder(base)
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty(NATIVE_UPDATE_MERGE_ENABLED, String.valueOf(updateMergeEnabled))
                .build();
    }

    @Test
    public void testUpdateOnNativeFailsFastWithClearMessage()
    {
        // The updatable catalog reports no "format-version" property, so the UPDATE-as-MERGE
        // rewrite does not apply and the legacy UpdateNode pipeline is the only option left.
        // On a native session that pipeline cannot run; the planner must reject it here
        // rather than letting the worker fail on the untranslatable plan node.
        try {
            getQueryRunner().inTransaction(nativeSession(getQueryRunner().getDefaultSession()), transactionSession ->
                    getQueryRunner().createPlan(
                            transactionSession,
                            UPDATE_SQL,
                            Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED,
                            WarningCollector.NOOP));
            fail("Expected planning UPDATE on a native session to fail");
        }
        catch (PrestoException e) {
            assertEquals(
                    e.getMessage(),
                    "UPDATE on the native (Prestissimo) engine is not supported for table default." + TABLE_NAME + ". " +
                            "The native engine executes UPDATE only through the MERGE pipeline, which requires the target " +
                            "table to support MERGE (for Iceberg, format-version 2 or 3). Run the query on a Java worker " +
                            "cluster, or use a table that supports MERGE.");
        }
    }

    @Test
    public void testUpdateOnNativeWithRewriteDisabledFailsFastToo()
    {
        // The disabled-property configuration is rejected even earlier, by
        // LogicalPlanner.createUpdatePlan, so it never reaches QueryPlanner. Assert the
        // composed behavior: every native UPDATE path fails fast with a clear message,
        // never by shipping an UpdateNode to a native worker.
        try {
            getQueryRunner().inTransaction(nativeSession(getQueryRunner().getDefaultSession(), false), transactionSession ->
                    getQueryRunner().createPlan(
                            transactionSession,
                            UPDATE_SQL,
                            Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED,
                            WarningCollector.NOOP));
            fail("Expected planning UPDATE on a native session with the merge rewrite disabled to fail");
        }
        catch (PrestoException e) {
            assertEquals(
                    e.getMessage(),
                    "UPDATE is not supported on native (Prestissimo) workers when " +
                            "native_update_merge_enabled is false. Enable that session property " +
                            "(or run on a Java cluster) to use UPDATE.");
        }
    }

    @Test
    public void testUpdateOnJavaSessionStillPlans()
    {
        // The same query on a Java session keeps planning through the legacy UpdateNode path.
        Plan plan = getQueryRunner().inTransaction(getQueryRunner().getDefaultSession(), transactionSession ->
                getQueryRunner().createPlan(
                        transactionSession,
                        UPDATE_SQL,
                        Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED,
                        WarningCollector.NOOP));
        long updateNodes = searchFrom(plan.getRoot())
                .where(UpdateNode.class::isInstance)
                .findAll()
                .size();
        assertEquals(updateNodes, 1, "Java session should keep planning UPDATE through UpdateNode");
    }

    private static class UpdatableConnectorFactory
            implements ConnectorFactory
    {
        @Override
        public String getName()
        {
            return CATALOG_NAME;
        }

        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return new TestingHandleResolver();
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            ConnectorMetadata metadata = new UpdatableTestingMetadata();
            return new Connector()
            {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
                {
                    return new ConnectorTransactionHandle() {};
                }

                @Override
                public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction)
                {
                    return metadata;
                }

                @Override
                public ConnectorSplitManager getSplitManager()
                {
                    return new TestingSplitManager(ImmutableList.of());
                }

                @Override
                public ConnectorPageSourceProvider getPageSourceProvider()
                {
                    return new TestingPageSourceProvider();
                }
            };
        }
    }

    /**
     * A {@link TestingMetadata} that supports the legacy UPDATE contract
     * ({@code beginUpdate} / {@code getUpdateRowIdColumn}) but reports no "format-version"
     * table property, so the native UPDATE-as-MERGE rewrite never applies to it. This is the
     * shape of every non-Iceberg updatable connector and of Iceberg V1 tables.
     */
    private static class UpdatableTestingMetadata
            extends TestingMetadata
    {
        private UpdatableTestingMetadata()
        {
            createTable(
                    TestingConnectorSession.SESSION,
                    new ConnectorTableMetadata(
                            new SchemaTableName("default", TABLE_NAME),
                            ImmutableList.of(
                                    ColumnMetadata.builder().setName("id").setType(IntegerType.INTEGER).build(),
                                    ColumnMetadata.builder().setName("name").setType(VarcharType.VARCHAR).build())),
                    false);
        }

        @Override
        public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns, Optional<RowExpression> updateScope)
        {
            return tableHandle;
        }

        @Override
        public Optional<ColumnHandle> getUpdateRowIdColumn(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
        {
            return Optional.empty();
        }
    }
}
