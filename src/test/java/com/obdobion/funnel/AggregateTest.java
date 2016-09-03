package com.obdobion.funnel;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * AggregateTest class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class AggregateTest
{

    /**
     * <p>
     * avgColAndEquError.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgColAndEquError()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        try
        {
            Funnel.sort(Helper.config(), " --col(string -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                    + " --orderby(key asc)"
                    + " --avg(myNumber -n avgNumber --equ 'myNumber')"
                    + " --format(key)(-s1)(avgNumber)");
            Assert.fail("Exception expected");
        } catch (final ParseException e)
        {
            Assert.assertEquals("aggregate \"avgnumber\" columnName and --equ are mutually exclusive", e.getMessage());
        }
    }

    /**
     * <p>
     * avgColDate.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgColDate()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 20160409");
        in1.add("a 20160411");
        in1.add("a 20160410");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(date -o2 -l8 -n myDate -d 'yyyyMMdd')"
                + " --orderby(key asc)"
                + " --avg(myDate -n myMin)"
                + " --format(key)(-s1)(-e myMin -l10 -d '%1$tm/%<td/%<tY')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 04/10/2016");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgColFloat.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgColFloat()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(float -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --avg(myNumber -n avg)"
                + " --format(key)(-s1)(-eavg -l3 -d '%03.0f')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 041");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgColHugeFloats.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgColHugeFloats()
            throws Throwable
    {
        final int rows = 50;
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        for (int i = 0; i < rows; i++)
            in1.add("a " + Long.MAX_VALUE);

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(float -o2 -l20 -n myHugeNumber)"
                + " --orderby(key asc)"
                + " --avg(myHugeNumber -n myAvgHugeNumber)"
                + " --format(key)(-s1)(-e myAvgHugeNumber -l20 -d '%020.0f')");

        Assert.assertEquals("records", rows, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 09223372036854776000");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgColHugeIntegers.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgColHugeIntegers()
            throws Throwable
    {
        final int rows = 50;
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        for (int i = 0; i < rows; i++)
            in1.add("a " + Long.MAX_VALUE);

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l20 -n myHugeNumber)"
                + " --orderby(key asc)"
                + " --avg(myHugeNumber -n myAvgHugeNumber)"
                + " --format(key)(-s1)(-e myAvgHugeNumber -l20 -d '%020d')");

        Assert.assertEquals("records", rows, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 09223372036854775807");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgColInt.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgColInt()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --avg(myNumber -n avg)"
                + " --format(key)(-s1)(-eavg -l3 -d '%03d')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 041");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgColIntWithAllNegatives.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgColIntWithAllNegatives()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a -1");
        in1.add("a -111");
        in1.add("a -11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l4 -n myNumber)"
                + " --orderby(key asc)"
                + " --avg(myNumber -n avg)"
                + " --format(key)(-s1)(-eavg -l3 -d '%03d')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a -41");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgColIntWithHugeNegatives.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgColIntWithHugeNegatives()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a " + (0 - Long.MAX_VALUE));
        in1.add("a " + (0 - Long.MAX_VALUE));
        in1.add("a " + (0 - Long.MAX_VALUE));

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l20 -n myNumber)"
                + " --orderby(key asc)"
                + " --avg(myNumber -n avg)"
                + " --format(key)(-s1)(-eavg -l20 -d '%020d')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a -9223372036854775807");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgColIntWithMixedSigns.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgColIntWithMixedSigns()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a " + (0 - Long.MAX_VALUE));
        in1.add("a " + (0 - Long.MAX_VALUE));
        in1.add("a " + (0 + Long.MAX_VALUE));

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l20 -n myNumber)"
                + " --orderby(key asc)"
                + " --avg(myNumber -n avg)"
                + " --format(key)(-s1)(-eavg -l20 -d '%020d')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a -3074457345618258602");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgEquDate.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgEquDate()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 20160409");
        in1.add("a 20160411");
        in1.add("a 20160410");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(date -o2 -l8 -n myDate -d 'yyyyMMdd')"
                + " --orderby(key asc)"
                + " --avg(-emyDate -n myMin)"
                + " --format(key)(-s1)(-e myMin -l10 -d '%1$tm/%<td/%<tY')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 04/10/2016");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgEquFloat.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgEquFloat()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(float -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --avg(-emyNumber -n avg)"
                + " --format(key)(-s1)(-eavg -l3 -d '%03.0f')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 041");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgEquHugeFloats.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgEquHugeFloats()
            throws Throwable
    {
        final int rows = 50;
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        for (int i = 0; i < rows; i++)
            in1.add("a " + Long.MAX_VALUE);

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(float -o2 -l20 -n myHugeNumber)"
                + " --orderby(key asc)"
                + " --avg(-emyHugeNumber -n myAvgHugeNumber)"
                + " --format(key)(-s1)(-e myAvgHugeNumber -l20 -d '%020.0f')");

        Assert.assertEquals("records", rows, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 09223372036854776000");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgEquHugeIntegers.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgEquHugeIntegers()
            throws Throwable
    {
        final int rows = 50;
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        for (int i = 0; i < rows; i++)
            in1.add("a " + Long.MAX_VALUE);

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l20 -n myHugeNumber)"
                + " --orderby(key asc)"
                + " --avg(-emyHugeNumber -n myAvgHugeNumber)"
                + " --format(key)(-s1)(-e myAvgHugeNumber -l20 -d '%020d')");

        Assert.assertEquals("records", rows, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 09223372036854775807");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgEquInt.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgEquInt()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --avg(-emyNumber -n avg)"
                + " --format(key)(-s1)(-eavg -l3 -d '%03d')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 041");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * avgStringError.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void avgStringError()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        try
        {
            Funnel.sort(Helper.config(), " --col(String -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                    + " --orderby(key asc)"
                    + " --avg(key -n avgNumber)"
                    + " --format(key)(-s1)(avgNumber)");
            Assert.fail("Exception expected");
        } catch (final ParseException e)
        {
            Assert.assertEquals("aggregate \"avgnumber\" must reference a numeric or date column: key (String)", e
                    .getMessage());
        }
    }

    /**
     * <p>
     * counter.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void counter()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");
        in1.add("z 22");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l3 -n col1)"
                + " --orderby(key asc)"
                + " --count(-n count)"
                + " --format(key)(-s1)(-ecount -l 2 -d '%02d')");

        Assert.assertEquals("records", 4L, context.getRecordCount());
        Assert.assertEquals("records", 2L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 03");
        expectedOut.add("z 01");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * counterOfNothing.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void counterOfNothing()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");
        in1.add("z 22");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --stopWhen 'recordNumber = 1'"
                + " --col(string -o0 -l1 -n key)(int -o2 -l3 -n col1)"
                + " --orderby(key asc)"
                + " --count(-n count)"
                + " --format(key)(-s1)(-ecount -l 2 -d '%02d')");

        Assert.assertEquals("records", 0L, context.getRecordCount());
        Assert.assertEquals("records", 0L, context.getWriteCount());

        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * duplicateAggregateName.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void duplicateAggregateName()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        try
        {
            Funnel.sort(Helper.config(), " --col(string -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                    + " --orderby(key asc)"
                    + " --count(-n badName)"
                    + " --avg(myNumber -n badName)"
                    + " --format(key)(-s1)(badName)");
            Assert.fail("Exception expected");
        } catch (final ParseException e)
        {
            Assert.assertEquals("aggregate \"badname\" must have a unique name", e.getMessage());
        }
    }

    /**
     * <p>
     * everythingVariable.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void everythingVariable()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 20160409 20170411");
        in1.add("a 20160411 20170410");
        in1.add("a 20160410 20160410");
        in1.add("c 20110409 20130411");
        in1.add("c 20110411 20120410");
        in1.add("b 20110410 20140410");
        in1.add("b 20120409 20110411");
        in1.add("b 20120411 20110410");
        in1.add("b 20120410 20100410");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel
                .sort(Helper.config(), file1.getAbsolutePath()
                        + " -o "
                        + output.getAbsolutePath()
                        + " --col(string -o0 -l1 -n key)(date -o2 -l8 -n myDate -d 'yyyyMMdd')(date -o11 -l8 -n myDate2 -d 'yyyyMMdd')"
                        + " --orderby(key asc)"
                        + " --avg(myDate -n myAvg)(myDate2 -n myAvg2)"
                        + " --min(myDate -n myMin)(myDate2 -n myMin2)"
                        + " --max(myDate -n myMax)(myDate2 -n myMax2)"
                        + " --count(-n myCount)"
                        + " --format(key)"
                        + " (-s1)(-e myCount -l4 -d '%04d')"
                        + " (-s1)(-e myMin  -l10 -d '%1$tm/%<td/%<tY')"
                        + " (-s1)(-e myMin2 -l10 -d '%1$tm/%<td/%<tY')"
                        + " (-s1)(-e myMax  -l10 -d '%1$tm/%<td/%<tY')"
                        + " (-s1)(-e myMax2 -l10 -d '%1$tm/%<td/%<tY')"
                        + " (-s1)(-e myAvg  -l10 -d '%1$tm/%<td/%<tY')"
                        + " (-s1)(-e myAvg2 -l10 -d '%1$tm/%<td/%<tY')");

        Assert.assertEquals("records", 9L, context.getRecordCount());
        Assert.assertEquals("records", 3L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 0003 04/09/2016 04/10/2016 04/11/2016 04/11/2017 04/10/2016 12/09/2016");
        expectedOut.add("b 0004 04/10/2011 04/10/2010 04/11/2012 04/10/2014 01/09/2012 10/10/2011");
        expectedOut.add("c 0002 04/09/2011 04/10/2012 04/11/2011 04/11/2013 04/10/2011 10/10/2012");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * maxColDate.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void maxColDate()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 20160409");
        in1.add("a 20160411");
        in1.add("a 20160410");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(date -o2 -l8 -n myDate -d 'yyyyMMdd')"
                + " --orderby(key asc)"
                + " --max(myDate -n myMin)"
                + " --format(key)(-s1)(-e myMin -l10 -d '%1$tm/%<td/%<tY')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 04/11/2016");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * maxColFloat.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void maxColFloat()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(float -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --max(myNumber -n myMin)"
                + " --format(key)(-s1)(-e myMin -l3 -d '%3.0f')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 111");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * maxColInt.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void maxColInt()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --max(myNumber -n myMin)"
                + " --format(key)(-s1)(-e myMin -l3 -d '%3d')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 111");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * maxEquDate.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void maxEquDate()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 20160409");
        in1.add("a 20160411");
        in1.add("a 20160410");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(date -o2 -l8 -n myDate -d 'yyyyMMdd')"
                + " --orderby(key asc)"
                + " --max(-emyDate -n myMin)"
                + " --format(key)(-s1)(-e myMin -l10 -d '%1$tm/%<td/%<tY')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 04/11/2016");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * maxEquFloat.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void maxEquFloat()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(float -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --max(-emyNumber -n myMin)"
                + " --format(key)(-s1)(-e myMin -l3 -d '%3.0f')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 111");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * maxEquInt.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void maxEquInt()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --max(-emyNumber -n myMin)"
                + " --format(key)(-s1)(-e myMin -l3 -d '%3d')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 111");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * minColDate.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void minColDate()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 20160409");
        in1.add("a 20160411");
        in1.add("a 20160410");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(date -o2 -l8 -n myDate -d 'yyyyMMdd')"
                + " --orderby(key asc)"
                + " --min(myDate -n myMin)"
                + " --format(key)(-s1)(-e myMin -l10 -d '%1$tm/%<td/%<tY')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 04/09/2016");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * minColFloat.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void minColFloat()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(float -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --min(myNumber -n myMin)"
                + " --format(key)(-s1)(-e myMin -l3 -d '%3.0f')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a   1");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * minColInt.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void minColInt()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --min(myNumber -n myMin)"
                + " --format(key)(-s1)(-e myMin -l3 -d '%3d')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a   1");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * minEquDate.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void minEquDate()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 20160409");
        in1.add("a 20160411");
        in1.add("a 20160410");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(date -o2 -l8 -n myDate -d 'yyyyMMdd')"
                + " --orderby(key asc)"
                + " --min(-emyDate -n myMin)"
                + " --format(key)(-s1)(-e myMin -l10 -d '%1$tm/%<td/%<tY')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 04/09/2016");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * minEquFloat.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void minEquFloat()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(float -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --min(-emyNumber -n myMin)"
                + " --format(key)(-s1)(-e myMin -l3 -d '%3.0f')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a   1");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * minEquInt.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void minEquInt()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --min(-emyNumber -n myMin)"
                + " --format(key)(-s1)(-e myMin -l3 -d '%3d')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a   1");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * sumColFloat.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void sumColFloat()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1.1");
        in1.add("a 111.1");
        in1.add("a 11.1");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(float -o2 -l5 -n myNumber)"
                + " --orderby(key asc)"
                + " --sum(myNumber -n sum)"
                + " --format(key)(-s1)(-esum -l7 -d '%03.3f')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 123.300");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * sumColInt.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void sumColInt()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --sum(myNumber -n sum)"
                + " --format(key)(-s1)(-esum -l5 -d '%03d')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 123");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * sumEquFloat.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void sumEquFloat()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1.1");
        in1.add("a 111.1");
        in1.add("a 11.1");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(float -o2 -l5 -n myNumber)"
                + " --orderby(key asc)"
                + " --sum(-emyNumber -n sum)"
                + " --format(key)(-s1)(-esum -l7 -d '%03.3f')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 123.300");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * sumEquInt.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void sumEquInt()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("a 1");
        in1.add("a 111");
        in1.add("a 11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o "
                + output.getAbsolutePath()
                + " --col(string -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                + " --orderby(key asc)"
                + " --sum(-emyNumber -n sum)"
                + " --format(key)(-s1)(-esum -l5 -d '%03d')");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a 123");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * unknownColumn.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void unknownColumn()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        try
        {
            Funnel.sort(Helper.config(), " --col(string -o0 -l1 -n key)(int -o2 -l3 -n myNumber)"
                    + " --orderby(key asc)"
                    + " --avg(unknownColumnName -n avgNumber)"
                    + " --format(key)(-s1)(avgNumber)");
            Assert.fail("Exception expected");
        } catch (final ParseException e)
        {
            Assert.assertEquals("aggregate \"avgnumber\" must reference a defined column: unknowncolumnname", e
                    .getMessage());
        }
    }
}
