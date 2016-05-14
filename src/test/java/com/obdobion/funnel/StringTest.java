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
public class StringTest
{

    @Test
    public void caseMatters ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("z");
        in1.add("a");
        in1.add("M");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
            + " -o " + output.getAbsolutePath()
            + " --col(string -o0 -l1 -n col1)"
            + " --orderby(col1 asc)");

        Assert.assertEquals("records", 3L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("M");
        expectedOut.add("a");
        expectedOut.add("z");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    @Test
    public void ignoreCase ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("z");
        in1.add("a");
        in1.add("M");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
            + " -o " + output.getAbsolutePath()
            + " --col(string -o0 -l1 -n col1)"
            + " --orderby(col1 aasc)");

        Assert.assertEquals("records", 3L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("a");
        expectedOut.add("M");
        expectedOut.add("z");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }
}
