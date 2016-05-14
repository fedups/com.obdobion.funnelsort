package com.obdobion.funnel;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;

/**
 * @author Chris DeGreef
 *
 */
public class FormatOutputTest
{
    @Test
    public void defaultFiller () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(comments -l10)(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        for (final String expLine : out)
        {
            exp.add(expLine.substring(6) + "    " + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void equColumnReferenceToString () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12346 line 1");
        out.add("54322 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(--equ \"toString(zipCode/2)\" -l 10)(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        exp.add("6173.0    12346");
        exp.add("27161.0   54322");

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void equColumnReferenceWithFormat () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12346 line 1");
        out.add("54322 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(--equ \"zipCode/2\" -l 10 -d'%5.0f')(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        exp.add(" 6173     12346");
        exp.add("27161     54322");

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void equFormatDate () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12346 line 1");
        out.add("54322 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(zipCode)(--equ \"date('20160510', 'yyyyMMdd')\" -l 19 -d'%tc')"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        exp.add("12346Tue May 10 00:00:00");
        exp.add("54322Tue May 10 00:00:00");

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void equSimpleString () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(--equ \"'WHAT'\" -l 4 -s 5)(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        for (final String expLine : out)
        {
            exp.add("WHAT " + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void largerOutputArea () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(comments -l2 -f' ' -s3)(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        for (final String expLine : out)
        {
            exp.add(expLine.substring(6, 8) + " " + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void offset () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(comments -o1 -l2)(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        for (final String expLine : out)
        {
            exp.add(expLine.substring(7, 9) + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void offsetNoLength () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2a");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(comments -o1)(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        for (final String expLine : out)
        {
            exp.add(expLine.substring(7) + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void oneColumnVariableLength () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)"
            + " --format(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        for (final String expLine : out)
        {
            exp.add(expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void onlyFiller () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12346 line 1");
        out.add("54322 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(zipCode)(-s3)(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        exp.add("12346   12346");
        exp.add("54322   54322");

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void truncate () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(comments -l2)(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        for (final String expLine : out)
        {
            exp.add(expLine.substring(6, 8) + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void twoColumnCSV () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12345,line 1");
        out.add("54321,line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        try
        {
            Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --col(int -f1 -l5 -n zipCode)(string -f2 -n comments)"
                + " --format(comments)(zipCode)"
                + " -r --csv() ");
        } catch (final ParseException e)
        {
            Assert.assertEquals("--csv and --format are mutually exclusive parameters", e.getMessage());
        } finally
        {
            Assert.assertTrue(file.delete());
        }
    }

    @Test
    public void twoColumnVariableLength () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -l6 -n comments)"
            + " --format(comments)(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        for (final String expLine : out)
        {
            exp.add(expLine.substring(6) + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void twoColumnVariableLengthImpliedFieldLength () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(comments)(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        for (final String expLine : out)
        {
            exp.add(expLine.substring(6) + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void xFiller () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile(testName, out, true);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
            + " --format(comments -l10 -f'x')(zipCode)"
            + " -r ");

        final List<String> exp = new ArrayList<>();
        for (final String expLine : out)
        {
            exp.add(expLine.substring(6) + "xxxx" + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

}
