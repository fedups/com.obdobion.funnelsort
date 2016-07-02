package com.obdobion.funnel;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses(
{
    AbbreviationTests.class,
    AggregateTest.class,
    BasicTest.class,
    BigTest.class,
    BinaryTest.class,
    ColumnTests.class,
    CommandLineOptionsTest.class,
    CompareTests.class,
    CsvTest.class,
    DuplicateTest.class,
    ExampleTest.class,
    FormatOutputTest.class,
    FormatTests.class,
    FunnelTest.class,
    HeaderTests.class,
    HexDumpTest.class,
    InputCacheTests.class,
    InputTest.class,
    MultiFileTest.class,
    RecordNumberTest.class,
    StopWhenTest.class,
    StringTest.class,
    WhereTest.class
})
public class MasterSuite
{
    //
}
