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
        Helper.initializeFor("TEST dupsFirst");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 130; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        out.add(in.get(0));

        final File file = Helper.createUnsortedFile("dupsFirst", in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 130 -f10"
                + "--col(String -o0,-l3 -n col1)"
                + "--orderby(col1) "
                + "--dup firstonly" + Helper.DEFAULT_OPTIONS);

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
        Helper.initializeFor("TEST dupsLast");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 130; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        out.add(in.get(129));

        final File file = Helper.createUnsortedFile("dupsLast", in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 130 -f10"
                + "--col(String -o0,-l3 -n col1)"
                + "--orderby(col1) "
                + "--dup lastonly" + Helper.DEFAULT_OPTIONS);

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
        Helper.initializeFor("TEST dupsReverse");

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

        final File file = Helper.createUnsortedFile("dupsReverse", in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 130 -f10 "
                + "--col(String -o0,-l3 -n col1)"
                + "--orderby(col1) "
                + "--dup reverse" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 130L, context.provider.actualNumberOfRows());
        Assert.assertEquals("dups", 129L, context.publisher.getDuplicateCount());
        Assert.assertEquals("write", 130L, context.publisher.getWriteCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }
}
