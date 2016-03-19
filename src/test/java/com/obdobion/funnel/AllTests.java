package com.obdobion.funnel;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author Chris DeGreef
 *
 */
@RunWith(Suite.class)
@SuiteClasses(
{
    BinaryTest.class, CompareTests.class, CsvTest.class, DuplicateTest.class, FormatTests.class,
    FunnelTest.class, InputCacheTests.class, InputTest.class, MultiFileTest.class, BigTest.class, WhereTest.class,
    VariableInputTest.class
})
public class AllTests
{
    //
}
