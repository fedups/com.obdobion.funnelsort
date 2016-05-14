package com.obdobion.funnel;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class ExampleTest
{
    @Test
    public void convertFixedToVariable () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void convertVariableToFixed () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void copyCollate () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 104L, context.getRecordCount());
        Assert.assertEquals("records out", 104L, context.getWriteCount());
        Assert.assertEquals("records dup", 52L, context.getDuplicateCount());
    }

    @Test
    public void copyOriginal () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void copyReverse () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void createFile () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = new File("c:/tmp/MyDataVariable.out");

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
        Assert.assertTrue("delete", output.delete());
    }

    @Test
    public void csvSort () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 53L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void dos2unix () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 23L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void dupFirstOnly () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 2L, context.getWriteCount());
        Assert.assertEquals("records dup", 21L, context.getDuplicateCount());
    }

    @Test
    public void eolWord () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 23L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void formatComputedColumn () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void formatDate () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 23L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void formatFiller () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void formatFormatNumber () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void formatTwoColumns () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void multiKey () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void oneKey () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 28L, context.getRecordCount());
        Assert.assertEquals("records out", 28L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void orderByAbsInt () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void orderByDescDate () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 23L, context.getWriteCount());
        Assert.assertEquals("records dup", 8L, context.getDuplicateCount());
    }

    @Test
    public void orderByFloat () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 5L, context.getDuplicateCount());
    }

    @Test
    public void orderByInt () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void sortMultipleFiles () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 58L, context.getRecordCount());
        Assert.assertEquals("records out", 58L, context.getWriteCount());
        Assert.assertEquals("records dup", 6L, context.getDuplicateCount());
    }

    @Test
    public void sortSingleFile () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 52L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void sortWildFile () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 58L, context.getRecordCount());
        Assert.assertEquals("records out", 58L, context.getWriteCount());
        Assert.assertEquals("records dup", 6L, context.getDuplicateCount());
    }

    @Test
    public void stopAtRecordNumber () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 10L, context.getRecordCount());
        Assert.assertEquals("records out", 10L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void stopAtTimestamp () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 19L, context.getRecordCount());
        Assert.assertEquals("records out", 19L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void upperCase () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 23L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void wherePattern () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 23L, context.getRecordCount());
        Assert.assertEquals("records out", 4L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

    @Test
    public void whereRange () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.getRecordCount());
        Assert.assertEquals("records out", 8L, context.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.getDuplicateCount());
    }

}
