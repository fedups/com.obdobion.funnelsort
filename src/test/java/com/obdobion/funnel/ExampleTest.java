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
    // @Test
    public void sortMultipleFiles () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File outFile = Helper.outFileWhenInIsSysin();

        final FunnelContext context = Funnel.sort(Helper.config(), "@"
            + testName
            + ".fun -o "
            + outFile.getAbsolutePath());
        /*
         * Only counts the last file in actual number of rows.
         */
        Assert.assertEquals("records in ", 52L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 52L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.publisher.getDuplicateCount());
        Assert.assertTrue("delete", outFile.delete());
    }

    @Test
    public void sortSingleFile () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File outFile = Helper.outFileWhenInIsSysin();

        final FunnelContext context = Funnel.sort(Helper.config(), "@"
            + testName
            + ".fun -o "
            + outFile.getAbsolutePath());

        Assert.assertEquals("records in ", 52L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records out", 52L, context.publisher.getWriteCount());
        Assert.assertEquals("records dup", 0L, context.publisher.getDuplicateCount());
        Assert.assertTrue("delete", outFile.delete());
    }

}
