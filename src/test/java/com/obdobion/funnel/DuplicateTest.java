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
public class DuplicateTest
{
    @Test
    public void dupsFirst ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 130; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        out.add(in.get(0));

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 130 --fixedIn 10"
                + "--col(String -o0,-l3 -n col1)"
                + "--orderby(col1) "
                + "--dup firstonly");

        Assert.assertEquals("records", 130L, context.provider.actualNumberOfRows());
        Assert.assertEquals("dups", 129L, context.publisher.getDuplicateCount());
        Assert.assertEquals("write", 1L, context.publisher.getWriteCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void dupsLast ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 130; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        out.add(in.get(129));

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 130 --fixedIn 10"
                + "--col(String -o0,-l3 -n col1)"
                + "--orderby(col1) "
                + "--dup lastonly");

        Assert.assertEquals("records", 130L, context.provider.actualNumberOfRows());
        Assert.assertEquals("dups", 129L, context.publisher.getDuplicateCount());
        Assert.assertEquals("write", 1L, context.publisher.getWriteCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void dupsReverse ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 130; r++)
        {
            in.add("row " + (r + 1000));
        }

        final List<String> out = new ArrayList<>();
        for (int r = 129; r >= 0; r--)
        {
            out.add("row " + (r + 1000));
        }

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 130 --fixedIn 10 "
                + "--col(String -o0,-l3 -n col1)"
                + "--orderby(col1) "
                + "--dup reverse");

        Assert.assertEquals("records", 130L, context.provider.actualNumberOfRows());
        Assert.assertEquals("dups", 129L, context.publisher.getDuplicateCount());
        Assert.assertEquals("write", 130L, context.publisher.getWriteCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }
}
