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
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 130 -c original --fixedIn 10");

        Assert.assertEquals("records", 130L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyOriginalFixed50 () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 50 -c original --fixedIn 10");

        Assert.assertEquals("records", 50L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyOriginalVar1000 () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 1000 -c original");

        Assert.assertEquals("records", 1000L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyOriginalVar130 () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 130 -c original");

        Assert.assertEquals("records", 130L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyOriginalVar50 () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 50 -c original");

        Assert.assertEquals("records", 50L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyReverseFixed () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 50 -c reverse --fixedIn 10");

        Assert.assertEquals("records", 50L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void copyReverseVar () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 50 -c reverse");

        Assert.assertEquals("records", 50L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortDate () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 50 --fixedIn 16"
                + "--co(-ndate Date -o0 --fo " + format + ")"
                + "--o(date)");

        Assert.assertEquals("records", 50L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortDateNoFormat () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        try
        {
            Funnel.sort(Helper.config(), "noteverread --max 50 --fixedin 10 --col(-ndate Date -o1 -l14)");
            Assert.fail("expected ParseException");
        } catch (final ParseException e)
        {
            Assert.assertEquals("file not found: noteverread ", e.getMessage());
        }
    }

    @Test
    public void sortFixed1000 () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --max 1000 --fixedIn 10"
                + "--col(-nc Integer -o4 -l4 )"
                + "--order(c desc)");

        Assert.assertEquals("records", 1000L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortFullKeyVariable1000Asc () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --power 4");

        Assert.assertEquals("records", 1000L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortIntVar1000 () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --power 4"
                + "--col(-nc Integer -o4 -l4)"
                + "--orderby(c desc)");

        Assert.assertEquals("records", 1000L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortStringVar1000 () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath()
                + " -r --power 4 "
                + "--col(-nn String -o4 -l4)"
                + "--order(n desc)");
        Assert.assertEquals("records", 1000L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void sortVarP2 () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final File file = Helper.createUnsortedFile(testName, in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath() + " -r --power 2"
                + "--col(-ncol1 Integer,-o4,-l2)"
                + "--ord(col1 desc) ");

        Assert.assertEquals("records", 10L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void unexpectedInput () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), "--ThisIsNotAValueArgument");
        } catch (final ParseException pe)
        {
            Assert.assertEquals("unexpected input: --ThisIsNotAValueArgument ", pe.getMessage());
        }
    }

    @Test
    public void usageMessage () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), "-?");
        } catch (final ParseException e)
        {
            System.out.println(e.getMessage());
        }
    }

    @Test
    public void version () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        Funnel.sort(Helper.config(), "--version");
    }
}
