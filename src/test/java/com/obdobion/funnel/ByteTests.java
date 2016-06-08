package com.obdobion.funnel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class ByteTests
{
    @Test
    public void asHex ()
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
                + "  (-e 'toHex(bytes)' -l 6)(string)"
                    );

            Assert.assertEquals("records", 1L, context.getWriteCount());
        } finally
        {
            Assert.assertTrue(file.delete());
        }
    }

    @Test
    public void asNumberInEqu ()
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
                + "  (-e'bytes*2' -l 3)(string)"
                    );
            Assert.fail("expected exception");
        } catch (final Exception e)
        {
            Assert.assertEquals("op(multiply); invalid type byte[]", e.getMessage());
        } finally
        {
            Assert.assertTrue(file.delete());
        }
    }

    @Test
    public void asSearchableField ()
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
                + "  (bytes)(string)"
                    );

            Assert.assertEquals("records", 1L, context.getWriteCount());
        } finally
        {
            Assert.assertTrue(file.delete());
        }
    }

    @Test
    public void basicUse ()
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
            + "  (bytes -l 3)(string)"
                );

        Assert.assertEquals("records", 2L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }
}
