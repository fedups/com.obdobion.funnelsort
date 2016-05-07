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

        final File file = Helper.createUnsortedFile("copyOriginalFixed130", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 130 -c original -f10" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 130L, context.provider.actualNumberOfRows());
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

        final File file = Helper.createUnsortedFile("copyOriginalFixed50", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + "  --max 50 -c original -f10" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 50L, context.provider.actualNumberOfRows());
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

        final File file = Helper.createUnsortedFile("copyOriginalVar1000", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 1000 -c original" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1000L, context.provider.actualNumberOfRows());
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

        final File file = Helper.createUnsortedFile("copyOriginalVar130", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 130 -c original" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 130L, context.provider.actualNumberOfRows());
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

        final File file = Helper.createUnsortedFile("copyOriginalVar50", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 50 -c original" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 50L, context.provider.actualNumberOfRows());
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

        final File file = Helper.createUnsortedFile("copyReverseFixed", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 50 -c reverse -f10" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 50L, context.provider.actualNumberOfRows());
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

        final File file = Helper.createUnsortedFile("copyReverseVar", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 50 -c reverse" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 50L, context.provider.actualNumberOfRows());
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

        final File file = Helper.createUnsortedFile("sortDate", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 50 -f16"
                + "--co(-ndate Date -o0 --fo " + format + ")"
                + "--o(date)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 50L, context.provider.actualNumberOfRows());
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
            Funnel.sort(Helper.config(), "noteverread --max 50 -f10 -k(Date -o1 -l14 asc)" + Helper.DEFAULT_OPTIONS);
            Assert.fail("expected missing date format error");
        } catch (final Throwable e)
        {
            // intentionally empty
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

        final File file = Helper.createUnsortedFile("sortFixed1000", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --max 1000 -f10"
                + "--col(-nc Integer -o4 -l4 )"
                + "--order(c desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1000L, context.provider.actualNumberOfRows());
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

        final File file = Helper.createUnsortedFile("sortFullKeyVariable1000Asc", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --power 4" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1000L, context.provider.actualNumberOfRows());
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

        final File file = Helper.createUnsortedFile("sortIntVar1000", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --power 4"
                + "--col(-nc Integer -o4 -l4)"
                + "--orderby(c desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1000L, context.provider.actualNumberOfRows());
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

        final File file = Helper.createUnsortedFile("sortStringVar1000", in);
        final FunnelContext context = Funnel.sort(Helper.config(),
            file.getAbsolutePath()
                + " -o" + file.getAbsolutePath()
                + " --power 4 "
                + "--col(-nn String -o4 -l4)"
                + "--order(n desc)"
                + Helper.DEFAULT_OPTIONS);
        Assert.assertEquals("records", 1000L, context.provider.actualNumberOfRows());
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

        final File file = Helper.createUnsortedFile("sortVarP2", in);
        final FunnelContext context = Funnel.sort(
            Helper.config(),
            file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --power 2"
                + "--col(-ncol1 Integer,-o4,-l2)"
                + "--ord(col1 desc) "
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 10L, context.provider.actualNumberOfRows());
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
            Funnel.sort(Helper.config(), "--ThisIsNotAValueArgument" + Helper.DEFAULT_OPTIONS);
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
        try
        {
            Funnel.sort(Helper.config(), "--version" + Helper.DEFAULT_OPTIONS);
        } catch (final ParseException pe)
        {
            Assert.assertEquals("unexpected input: --version ", pe.getMessage());
        }
    }
}
