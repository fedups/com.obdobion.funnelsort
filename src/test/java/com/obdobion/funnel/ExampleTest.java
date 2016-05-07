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
    public void createFile () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = new File("c:/tmp/MyDataVariable.out");

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 52L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.publisher.getDuplicateCount());
        Assert.assertTrue("delete", output.delete());
    }

    @Test
    public void multiKey () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 52L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.publisher.getDuplicateCount());
    }

    @Test
    public void oneKey () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 28L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 28L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.publisher.getDuplicateCount());
    }

    @Test
    public void orderByAbsInt () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 52L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.publisher.getDuplicateCount());
    }

    @Test
    public void orderByDescDate () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 24L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 24L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 9L, context.publisher.getDuplicateCount());
    }

    @Test
    public void orderByFloat () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 52L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 5L, context.publisher.getDuplicateCount());
    }

    @Test
    public void orderByInt () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 52L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.publisher.getDuplicateCount());
    }

    @Test
    public void sortMultipleFiles () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 30L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 58L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 6L, context.publisher.getDuplicateCount());
    }

    @Test
    public void sortSingleFile () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 52L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 52L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.publisher.getDuplicateCount());
    }

    @Test
    public void sortWildFile () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final FunnelContext context = Funnel.sort(Helper.config(), "@" + testName + ".fun");

        Assert.assertEquals("records in ", 30L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 58L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 6L, context.publisher.getDuplicateCount());
    }

}
