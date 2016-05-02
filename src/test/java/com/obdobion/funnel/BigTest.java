package com.obdobion.funnel;

import java.io.File;
import java.io.IOException;
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
    public void cachingMulitpleInputFiles ()
        throws Throwable
    {
        Helper.initializeFor("TEST cachingMulitpleInputFiles");

        final File output = new File("/tmp/cachingMulitpleInputFiles");

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

        final File file = Helper.createUnsortedFile("cachingMulitpleInputFiles1", in1);
        final File file2 = Helper.createUnsortedFile("cachingMulitpleInputFiles2", in1);

        try
        {
            Funnel.sort(file.getParentFile().getAbsolutePath()
                + "/cachingMulitpleInputFiles* "
                + " --col(int -o0 -l7 -n col1)"
                + " --orderby(col1 desc)"
                + " -o "
                + output.getAbsolutePath()
                + " --max 5000000 "
                + " --pow 16 --cacheInput --cacheWork"
                + Helper.DEFAULT_OPTIONS);
            Assert.fail("expected IOException");
        } catch (final IOException e)
        {
            Assert.assertEquals("the cacheInput option is not allowed with multiple input files", e.getMessage());
        } finally
        {
            Assert.assertTrue(file.delete());
            Assert.assertTrue(file2.delete());
        }
    }

    @Test
    public void multipleInputFiles ()
        throws Throwable
    {
        Helper.initializeFor("TEST multipleInputFiles");

        final File output = new File("/tmp/multipleInputFiles");

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
            expectedOut.add("" + x);
            expectedOut.add("" + x);
        }

        final File file = Helper.createUnsortedFile("multipleInputFiles1", in1);
        final File file2 = Helper.createUnsortedFile("multipleInputFiles2", in1);
        final File file3 = Helper.createUnsortedFile("multipleInputFiles3", in1);
        final File file4 = Helper.createUnsortedFile("multipleInputFiles4", in1);
        final File file5 = Helper.createUnsortedFile("multipleInputFiles5", in1);

        final FunnelContext context = Funnel.sort(file.getParentFile().getAbsolutePath() + "/multipleInputFiles* "
            + " --col(int -o0 -l7 -n col1)"
            + " --orderby(col1 desc)"
            + " -o "
            + output.getAbsolutePath()
            + " --max 5000000 "
            + " --pow 16 --cacheWork"
            + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 5000000L, context.publisher.getWriteCount());
        Helper.compare(output, expectedOut);

        Assert.assertTrue(file.delete());
        Assert.assertTrue(file2.delete());
        Assert.assertTrue(file3.delete());
        Assert.assertTrue(file4.delete());
        Assert.assertTrue(file5.delete());
        Assert.assertTrue(output.delete());
    }

    @Test
    public void oneBigFile ()
        throws Throwable
    {
        Helper.initializeFor("TEST oneBigFile");

        final File output = new File("/tmp/oneBigFile");

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

        final File file = Helper.createUnsortedFile("oneBigFile", in1);

        final FunnelContext context = Funnel.sort(file.getAbsolutePath()
            + " --col(int -o0 -l7 -n col1)"
            + " --orderby(col1 desc)"
            + " -o "
            + output.getAbsolutePath()
            + " --pow 8 --cacheWork --cacheInput"
            + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1000000L, context.publisher.getWriteCount());
        Helper.compare(output, expectedOut);

        Assert.assertTrue(file.delete());
        Assert.assertTrue(output.delete());
    }
}
