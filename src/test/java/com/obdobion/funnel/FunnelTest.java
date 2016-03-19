package com.obdobion.funnel;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.Funnel;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 * 
 */
public class FunnelTest
{
    @Test
    public void copyOriginalFixed130 () throws Throwable
    {
        Helper.initializeFor("TEST copyOriginalFixed130");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 130; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 0; r < 130; r++)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 130 -c original -f10" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 130L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyOriginalFixed50 () throws Throwable
    {
        Helper.initializeFor("TEST copyOriginalFixed50");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 50; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 0; r < 50; r++)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + "  --max 50 -c original -f10" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 50L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyOriginalVar1000 () throws Throwable
    {
        Helper.initializeFor("TEST copyOriginalVar1000");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 1000; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 0; r < 1000; r++)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 1000 -c original" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1000L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyOriginalVar130 () throws Throwable
    {
        Helper.initializeFor("TEST copyOriginalVar130");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 130; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 0; r < 130; r++)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 130 -c original" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 130L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyOriginalVar50 () throws Throwable
    {
        Helper.initializeFor("TEST copyOriginalVar50");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 50; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 0; r < 50; r++)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 50 -c original" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 50L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyReverseFixed () throws Throwable
    {
        Helper.initializeFor("TEST copyReverseFixed");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 50; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 49; r > 0; r--)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 50 -c reverse -f10" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 50L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyReverseVar () throws Throwable
    {
        Helper.initializeFor("TEST copyReverseVar");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 50; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 49; r > 0; r--)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 50 -c reverse" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 50L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortDate () throws Throwable
    {
        Helper.initializeFor("TEST sortDate");
        final String format = "yyyyHHmmssMMdd";
        final SimpleDateFormat sdf = new SimpleDateFormat(format);

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 50; r++)
        {
            final Calendar cal = Calendar.getInstance();
            cal.add(Calendar.DAY_OF_MONTH, -r);
            in.add(sdf.format(cal.getTime()));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 49; r >= 0; r--)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 50 -f16 -k(Date -o0 asc --fo " + format + ")" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 50L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortDateNoFormat () throws Throwable
    {
        Helper.initializeFor("TEST sortDateNoFormat");

        try
        {
            Funnel.sort("noteverread --max 50 -f10 -k(Date -o1 -l14 asc)" + Helper.DEFAULT_OPTIONS);
            Assert.fail("expected missing date format error");
        } catch (final Throwable e)
        {
            // intentionally empty
        }
    }

    @Test
    public void sortFixed1000 () throws Throwable
    {
        Helper.initializeFor("TEST sortFixed1000");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 1000; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 999; r > 0; r--)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 1000 -f10 -k(Integer -o4 -l4 desc)" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1000L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortFullKeyVariable1000Asc () throws Throwable
    {
        Helper.initializeFor("TEST sortFullKeyVariable1000Asc");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 1000; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 0; r > 1000; r++)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --power 4" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1000L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortIntVar1000 () throws Throwable
    {
        Helper.initializeFor("TEST sortIntVar1000");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 1000; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 999; r > 0; r--)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --power 4 -k(Integer -o4 -l4 desc)" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1000L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortStringVar1000 () throws Throwable
    {
        Helper.initializeFor("TEST sortVar1000");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 1000; r++)
        {
            in.add("row " + (r + 1000));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 999; r > 0; r--)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --power 4 -k(String -o4 -l4 desc)" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1000L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortVarP2 () throws Throwable
    {
        Helper.initializeFor("TEST sortVarP2");

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 10; r++)
        {
            in.add("row " + (r + 10));
        }
        final List<String> out = new ArrayList<>();
        for (int r = 9; r > 0; r--)
        {
            out.add(in.get(r));
        }

        final File file = Helper.createUnsortedFile(in);
        final FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --power 2 -k(Integer,-o4,-l2,desc)" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 10L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void unexpectedInput () throws Throwable
    {
        Helper.initializeFor("TEST unexpectedInput");
        try
        {
            Funnel.sort("--ThisIsNotAValueArgument" + Helper.DEFAULT_OPTIONS);
        } catch (final ParseException pe)
        {
            Assert.assertEquals("unexpected input: --ThisIsNotAValueArgument ", pe.getMessage());
        }
    }

    @Test
    public void usageMessage () throws Throwable
    {
        Helper.initializeFor("usageMessage");

        try
        {
            System.setProperty("specPath", "nowhere/specs;~/funnel;/etc/funnel/specs;/tmp");
            Funnel.sort("-?");
        } catch (final ParseException e)
        {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void version () throws Throwable
    {
        Helper.initializeFor("TEST version");
        try
        {
            Funnel.sort("--version" + Helper.DEFAULT_OPTIONS);
        } catch (final ParseException pe)
        {
            Assert.assertEquals("unexpected input: --version ", pe.getMessage());
        }
    }
}
