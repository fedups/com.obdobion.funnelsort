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
public class BigTest
{

    @Test
    public void multipleInputFiles ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 1; x <= 1000000; x++)
        {
            in1.add("" + x);
        }

        final List<String> expectedOut = new ArrayList<>();
        for (int x = 1000000; x >= 1; x--)
        {
            expectedOut.add("" + x);
            expectedOut.add("" + x);
            expectedOut.add("" + x);
        }

        final File file = Helper.createUnsortedFile(testName + "1", in1);
        final File file2 = Helper.createUnsortedFile(testName + "2", in1);
        final File file3 = Helper.createUnsortedFile(testName + "3", in1);

        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getParentFile().getAbsolutePath() + "/multipleInputFiles* "
                + " --col(int -o0 -l7 -n col1)"
                + " --orderby(col1 desc)"
                + " -o " + output.getAbsolutePath()
                + " --max 5000000 "
                + " --pow 16");

        Assert.assertEquals("records", 3000000L, context.getRecordCount());
        Assert.assertEquals("records", 3000000L, context.getWriteCount());
        Helper.compare(output, expectedOut);

        Assert.assertTrue(file.delete());
        Assert.assertTrue(file2.delete());
        Assert.assertTrue(file3.delete());
        Assert.assertTrue(output.delete());
    }

    @Test
    public void oneBigFile ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 1; x <= 1000000; x++)
        {
            in1.add("" + x);
        }

        final List<String> expectedOut = new ArrayList<>();
        for (int x = 1000000; x >= 1; x--)
        {
            expectedOut.add("" + x);
        }

        final File file = Helper.createUnsortedFile(testName, in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -o " + output.getAbsolutePath()
            + " --col(int -o0 -l7 -n col1)"
            + " --orderby(col1 desc)"
            + " --pow 8");

        Assert.assertEquals("records", 1000000L, context.getRecordCount());
        Assert.assertEquals("records", 1000000L, context.getWriteCount());
        Helper.compare(output, expectedOut);

        Assert.assertTrue(file.delete());
        Assert.assertTrue(output.delete());
    }
}
