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
 * ByteTests class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class ByteTests
{
    /**
     * <p>
     * asHex.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void asHex()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("a\t\t\t");
        logFile.add("b\r\r\r");

        final File file = Helper.createUnsortedFile(testName, logFile);

        try
        {
            final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                    + " --col"
                    + " (String -l1 -n string)"
                    + " (Byte -l3 -n bytes)"
                    + " --where \"not(empty(matches(bytes, '\t+')))\""
                    + " --orderBy (bytes desc)"
                    + " --formatOut"
                    + "  (-e 'toHex(bytes)' -l 6)(string)");

            Assert.assertEquals("records", 1L, context.getWriteCount());
        } finally
        {
            Assert.assertTrue(file.delete());
        }
    }

    /**
     * <p>
     * asNumberInEqu.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void asNumberInEqu()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("a\t\t\t");
        logFile.add("b\r\r\r");

        final File file = Helper.createUnsortedFile(testName, logFile);

        try
        {
            Funnel.sort(Helper.config(), file.getAbsolutePath()
                    + " --col"
                    + " (String -l1 -n string)"
                    + " (Byte -l3 -n bytes)"
                    + " --orderBy (bytes desc)"
                    + " --formatOut"
                    + "  (-e'bytes*2' -l 3)(string)");
            Assert.fail("expected exception");
        } catch (final Exception e)
        {
            Assert.assertEquals("op(multiply); invalid type byte[]", e.getMessage());
        } finally
        {
            Assert.assertTrue(file.delete());
        }
    }

    /**
     * <p>
     * asSearchableField.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void asSearchableField()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("a\t\t\t");
        logFile.add("b\r\r\r");

        final File file = Helper.createUnsortedFile(testName, logFile);

        try
        {
            final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                    + " --col"
                    + " (String -l1 -n string)"
                    + " (Byte -l3 -n bytes)"
                    + " --where \"not(empty(matches(bytes, '\t+')))\""
                    + " --orderBy (bytes desc)"
                    + " --formatOut"
                    + "  (bytes)(string)");

            Assert.assertEquals("records", 1L, context.getWriteCount());
        } finally
        {
            Assert.assertTrue(file.delete());
        }
    }

    /**
     * <p>
     * basicUse.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void basicUse()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("a\t\t\t");
        logFile.add("b\r\r\r");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --col"
                + " (String -l1 -n string)"
                + " (Byte -l3 -n bytes)"
                + " --orderBy (bytes desc)"
                + " --formatOut"
                + "  (bytes -l 3)(string)");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }
}
