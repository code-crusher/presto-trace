package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.ORDERS;

@Test(singleThreaded = true)
public class TestRowIdProbe
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(ImmutableList.of(ORDERS, CUSTOMER), ImmutableMap.of(), Optional.empty());
    }

    @Test
    public void probe()
    {
        Session on = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING, "true").build();
        Session off = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING, "false").build();

        ImmutableList<String> queries = ImmutableList.of(
                "SELECT \"$row_id\", count(*) FROM orders GROUP BY 1",
                "SELECT customer.\"$row_id\", COUNT(orders.orderkey) FROM customer LEFT JOIN orders ON customer.custkey = orders.custkey GROUP BY 1",
                "SELECT count(*) FROM orders a JOIN orders b ON a.\"$row_id\" = b.\"$row_id\"",
                "SELECT DISTINCT \"$row_id\" FROM orders");

        for (String q : queries) {
            String a = (String) computeActual(on, "EXPLAIN (TYPE DISTRIBUTED) " + q).getOnlyValue();
            String b = (String) computeActual(off, "EXPLAIN (TYPE DISTRIBUTED) " + q).getOnlyValue();
            String c = (String) computeActual(on, "EXPLAIN (TYPE DISTRIBUTED) " + q).getOnlyValue();
            System.out.println("PROBE on==off: " + a.equals(b) + " on==on: " + a.equals(c) + " :: " + q);
        }
    }
}
