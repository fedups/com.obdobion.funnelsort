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
public class BasicTest
{

    @Test
    public void integerSort()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("1");
        in1.add("111");
        in1.add("11");

        final File file1 = Helper.createUnsortedFile(testName, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o " + output.getAbsolutePath()
                + " --col(int -o0 -l7 -n col1)"
                + " --orderby(col1 asc)"
                + " --pow 2"
                + " --row 3");

        Assert.assertEquals("records", 3L, context.getRecordCount());
        Assert.assertEquals("records", 3L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("1");
        expectedOut.add("11");
        expectedOut.add("111");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    @Test
    public void multifileInputIntegerSort()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("1");
        in1.add("111");
        in1.add("21");

        final File file1 = Helper.createUnsortedFile(testName + "1", in1);
        final File file2 = Helper.createUnsortedFile(testName + "2", in1);
        final FunnelContext context = Funnel.sort(Helper.config(),
                file1.getAbsolutePath() + " " + file2.getAbsolutePath()
                        + " --nocacheinput"
                        + " --col(int -o0 -l3 -n col1)"
                        + " --orderby(col1 desc)"
                        + " --pow 2"
                        + " --row 6"
                        + " -o " + output.getAbsolutePath());

        Assert.assertEquals("records", 6L, context.getRecordCount());
        Assert.assertEquals("records", 6L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("111");
        expectedOut.add("111");
        expectedOut.add("21");
        expectedOut.add("21");
        expectedOut.add("1");
        expectedOut.add("1");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(file2.delete());
        Assert.assertTrue(output.delete());
    }

    @Test
    public void multifileInputStringSort()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("1");
        in1.add("111");
        in1.add("21");

        final File file1 = Helper.createUnsortedFile(testName + "1", in1);
        final File file2 = Helper.createUnsortedFile(testName + "2", in1);
        final FunnelContext context = Funnel.sort(Helper.config(),
                file1.getAbsolutePath() + " " + file2.getAbsolutePath()
                        + " --nocacheinput"
                        + " --col(String -o0 -l3 -n col1)"
                        + " --orderby(col1 desc)"
                        + " --pow 2"
                        + " --row 6"
                        + " -o " + output.getAbsolutePath());

        Assert.assertEquals("records", 6L, context.getRecordCount());
        Assert.assertEquals("records", 6L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();
        expectedOut.add("21");
        expectedOut.add("21");
        expectedOut.add("111");
        expectedOut.add("111");
        expectedOut.add("1");
        expectedOut.add("1");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(file2.delete());
        Assert.assertTrue(output.delete());
    }

}
