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
package com.facebook.presto.sql.query;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestCaseMixedNumericTypes
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testSimpleCaseWithMixedWhenOperandTypes()
    {
        // The when clause operands have different types (decimal(2,1) for "if" and double for
        // "coalesce"), so the case operand and every when operand must be coerced to a single
        // common type. Coercing the case operand with each when operand pairwise instead leaves
        // the operand of an earlier when clause with a different type, which results in invalid
        // bytecode at runtime ("java.lang.VerifyError: Bad type on operand stack").
        assertions.assertQuery(
                "SELECT CASE 1.0 WHEN if(MIN(true), -1, -1.0) THEN 1.0 WHEN coalesce(-c0, 1.0) THEN 1.0 END FROM (VALUES (DOUBLE '1.0'), (DOUBLE '2.0'), (DOUBLE '3.0')) t(c0) GROUP BY c0",
                "VALUES (CAST(NULL AS DECIMAL(2,1))), (CAST(NULL AS DECIMAL(2,1))), (CAST(NULL AS DECIMAL(2,1)))");

        // The same query with the when clauses in reverse order
        assertions.assertQuery(
                "SELECT CASE 1.0 WHEN coalesce(-c0, 1.0) THEN 1.0 WHEN if(MIN(true), -1, -1.0) THEN 1.0 END FROM (VALUES (DOUBLE '1.0'), (DOUBLE '2.0'), (DOUBLE '3.0')) t(c0) GROUP BY c0",
                "VALUES (CAST(NULL AS DECIMAL(2,1))), (CAST(NULL AS DECIMAL(2,1))), (CAST(NULL AS DECIMAL(2,1)))");

        // The case operand is coerced to double by the first when clause while the second
        // when clause operand is an integer
        assertions.assertQuery(
                "SELECT CASE 1 WHEN c0 THEN 'matches-c0' WHEN 1 THEN 'one' ELSE 'other' END FROM (VALUES (DOUBLE '1.0'), (DOUBLE '2.0'), (DOUBLE '3.0')) t(c0)",
                "VALUES (CAST('matches-c0' AS VARCHAR(10))), (CAST('one' AS VARCHAR(10))), (CAST('one' AS VARCHAR(10)))");

        // The when clause operands are coerced to the type of the case operand
        assertions.assertQuery(
                "SELECT CASE c0 WHEN 1 THEN 'one' WHEN 2.0 THEN 'two' ELSE 'other' END FROM (VALUES (DOUBLE '1.0'), (DOUBLE '2.0')) t(c0)",
                "VALUES (CAST('one' AS VARCHAR(5))), (CAST('two' AS VARCHAR(5)))");
    }
}
