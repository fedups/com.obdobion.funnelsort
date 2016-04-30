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
    public void oneColumnVariableLength () throws Throwable
    {
        Helper.initializeFor("TEST oneColumnVariableLength");

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile("oneColumnVariableLength", out, true);

        Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)"
                + " --format(zipCode)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        final List<String> exp = new ArrayList<>();
        for (String expLine : out)
        {
            exp.add(expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void twoColumnVariableLength () throws Throwable
    {
        Helper.initializeFor("TEST twoColumnVariableLength");

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile("twoColumnVariableLength", out, true);

        Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)(string -o6 -l6 -n comments)"
                + " --format(comments)(zipCode)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        final List<String> exp = new ArrayList<>();
        for (String expLine : out)
        {
            exp.add(expLine.substring(6) + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void twoColumnVariableLengthImpliedFieldLength () throws Throwable
    {
        Helper.initializeFor("TEST twoColumnVariableLength");

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile("twoColumnVariableLengthImpliedFieldLength", out, true);

        Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
                + " --format(comments)(zipCode)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        final List<String> exp = new ArrayList<>();
        for (String expLine : out)
        {
            exp.add(expLine.substring(6) + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void defaultFiller () throws Throwable
    {
        Helper.initializeFor("TEST defaultFiller");

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile("defaultFiller", out, true);

        Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
                + " --format(comments -l10)(zipCode)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        final List<String> exp = new ArrayList<>();
        for (String expLine : out)
        {
            exp.add(expLine.substring(6) + "    " + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void largerOutputArea () throws Throwable
    {
        Helper.initializeFor("TEST largerOutputArea");

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile("largerOutputArea", out, true);

        Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
                + " --format(comments -l2 -f' ' -s3)(zipCode)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        final List<String> exp = new ArrayList<>();
        for (String expLine : out)
        {
            exp.add(expLine.substring(6, 8) + " " + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void xFiller () throws Throwable
    {
        Helper.initializeFor("TEST xFiller");

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile("defaultFiller", out, true);

        Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
                + " --format(comments -l10 -f'x')(zipCode)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        final List<String> exp = new ArrayList<>();
        for (String expLine : out)
        {
            exp.add(expLine.substring(6) + "xxxx" + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void truncate () throws Throwable
    {
        Helper.initializeFor("TEST truncate");

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile("truncate", out, true);

        Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
                + " --format(comments -l2)(zipCode)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        final List<String> exp = new ArrayList<>();
        for (String expLine : out)
        {
            exp.add(expLine.substring(6, 8) + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void offset () throws Throwable
    {
        Helper.initializeFor("TEST offset");

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2");

        final File file = Helper.createUnsortedFile("offset", out, true);

        Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
                + " --format(comments -o1 -l2)(zipCode)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        final List<String> exp = new ArrayList<>();
        for (String expLine : out)
        {
            exp.add(expLine.substring(7, 9) + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void offsetNoLength () throws Throwable
    {
        Helper.initializeFor("TEST offset");

        final List<String> out = new ArrayList<>();
        out.add("12345 line 1");
        out.add("54321 line 2a");

        final File file = Helper.createUnsortedFile("offset", out, true);

        Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)(string -o6 -n comments)"
                + " --format(comments -o1)(zipCode)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        final List<String> exp = new ArrayList<>();
        for (String expLine : out)
        {
            exp.add(expLine.substring(7) + expLine.substring(0, 5));
        }

        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void twoColumnCSV () throws Throwable
    {
        Helper.initializeFor("TEST twoColumnCSV");

        final List<String> out = new ArrayList<>();
        out.add("12345,line 1");
        out.add("54321,line 2");

        final File file = Helper.createUnsortedFile("twoColumnCSV", out, true);

        try
        {
            Funnel.sort(file.getAbsolutePath()
                    + " --col(int -f0 -l5 -n zipCode)(string -f1 -n comments)"
                    + " --format(comments)(zipCode)"
                    + " -r --csv() --eol cr,lf "
                    + Helper.DEFAULT_OPTIONS);
        } catch (ParseException e)
        {
            Assert.assertEquals("--csv and --format are mutually exclusive parameters", e.getMessage());
        } finally
        {
            Assert.assertTrue(file.delete());
        }
    }

}
