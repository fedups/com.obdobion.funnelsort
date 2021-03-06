package com.obdobion.funnel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * ColumnTests class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class ColumnTests
{
    /**
     * <p>
     * notLastColumnInVLR.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void notLastColumnInVLR()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("  FATALX");
        logFile.add("  INFO X");
        logFile.add("  WARN X");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --col(string -o2 -l5 -n level)"
                + " --where \"(rtrim(level) = 'INFO')\"");

        Assert.assertEquals("records", 1L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * notStartingInFirstColumn.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void notStartingInFirstColumn()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("  FATAL");
        logFile.add("  INFO ");
        logFile.add("  WARN ");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --col(string -o2 -l5 -n level)"
                + " --where \"(level = 'FATAL')\""
                + " -r ");

        Assert.assertEquals("records", 1L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * shortValue.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void shortValue()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("  FATAL");
        logFile.add("  INFO ");
        logFile.add("  WARN ");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --col(string -o2 -l5 -n level)"
                + " --where \"(rtrim(level) = 'INFO')\"");

        Assert.assertEquals("records", 1L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * startingInFirstColumn.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void startingInFirstColumn()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("FATAL");
        logFile.add("INFO ");
        logFile.add("WARN ");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --col(string -o0 -l5 -n level)"
                + " --where \"(level = 'FATAL')\""
                + " -r ");

        Assert.assertEquals("records", 1L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }
}
