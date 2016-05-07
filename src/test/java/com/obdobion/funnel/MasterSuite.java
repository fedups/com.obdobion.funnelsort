package com.obdobion.funnel;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses(
{
    BasicTest.class,
    BigTest.class,
    BinaryTest.class,
    ColumnTests.class,
    CommandLineOptionsTest.class,
    CompareTests.class,
    CsvTest.class,
    DuplicateTest.class,
    FormatOutputTest.class,
    FormatTests.class,
    FunnelTest.class,
    InputCacheTests.class,
    InputTest.class,
    MultiFileTest.class,
    StopWhenTest.class,
    StringTest.class,
    VariableInputTest.class,
    WhereTest.class
})
public class MasterSuite
{
    //
}
