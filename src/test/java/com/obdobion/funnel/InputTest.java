package com.obdobion.funnel;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringBufferInputStream;
import java.text.ParseException;
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
@SuppressWarnings("deprecation")
public class InputTest
{
    @Test
    public void dos2unix () throws Throwable
    {
        Helper.initializeFor("TEST dos2unix");

        final List<String> out = new ArrayList<>();
        out.add("line 1");
        out.add("line 2");

        final StringBuilder sb = new StringBuilder();
        for (final String outline : out)
        {
            sb.append(outline).append(System.getProperty("line.separator"));
        }

        final File file = Helper.createUnsortedFile(out);

        final FunnelContext context = Funnel.sort(file.getAbsolutePath()
                + " --replace --max 2 -c original --eol CR LF --eolOut LF" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        // file.delete();
    }

    @Test
    public void emptySysin () throws Throwable
    {
        Helper.initializeFor("TEST fixedEmptySysin");

        final String out = "";

        final InputStream inputStream = new StringBufferInputStream(out);
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort("-f6--max 2 " + Helper.DEFAULT_OPTIONS);
        Assert.assertEquals("records", 0L, context.provider.actualNumberOfRows());
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void emptySysinNoMax () throws Throwable
    {
        Helper.initializeFor("TEST emptySysinNoMax");

        final String out = "";

        final InputStream inputStream = new StringBufferInputStream(out);
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort("" + Helper.DEFAULT_OPTIONS);
        Assert.assertEquals("records", 0L, context.provider.actualNumberOfRows());
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void everythingDefaults () throws Throwable
    {
        Helper.initializeFor("TEST everythingDefaults");

        final List<String> out = new ArrayList<>();
        out.add("line 1");
        out.add("line 2");

        final StringBuilder sb = new StringBuilder();
        for (final String outline : out)
        {
            sb.append(outline).append(System.getProperty("line.separator"));
        }
        final InputStream inputStream = new StringBufferInputStream(sb.toString());
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort();

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void fixedSysinSysout () throws Throwable
    {
        Helper.initializeFor("TEST fixedSysinSysout");

        final String out = "line 1line 2";

        final InputStream inputStream = new StringBufferInputStream(out);
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort("-f6--max 2 -c original" + Helper.DEFAULT_OPTIONS);
        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compareFixed(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void replaceErrorOut () throws Throwable
    {
        Helper.initializeFor("TEST fixedReplaceErrorOut");

        final String out = "line 1line 2";

        final InputStream inputStream = new StringBufferInputStream(out);
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        try
        {
            Funnel.sort("--replace -f6--max 2 -k(S)" + Helper.DEFAULT_OPTIONS);
            Assert.fail("Expected error");
        } catch (final ParseException e)
        {
            Assert.assertEquals(
                    "error msg",
                    "--replace requires --inputFile, redirection or piped input is not allowed",
                    e.getMessage());
        } finally
        {
            Assert.assertFalse("delete " + file.getAbsolutePath(), file.delete());
        }
    }

    @Test
    public void replaceInput () throws Throwable
    {
        Helper.initializeFor("TEST replaceInput");

        final List<String> out = new ArrayList<>();
        out.add("line 1");
        out.add("line 2");

        final StringBuilder sb = new StringBuilder();
        for (final String outline : out)
        {
            sb.append(outline).append(System.getProperty("line.separator"));
        }

        final File file = Helper.createUnsortedFile(out);

        final FunnelContext context = Funnel.sort(file.getAbsolutePath()
                + " --replace --max 2 -c original --eol CR LF" + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void replacePipedInputNotAllowed () throws Throwable
    {
        Helper.initializeFor("TEST replacePipedInputNotAllowed");

        final String out = "line 1line 2";

        final InputStream inputStream = new StringBufferInputStream(out);
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        try
        {
            Funnel.sort("--replace -f6--max 2 -k(String)" + Helper.DEFAULT_OPTIONS);
            Assert.fail("Expected error");
        } catch (final ParseException e)
        {
            Assert.assertEquals(
                    "error msg",
                    "--replace requires --inputFile, redirection or piped input is not allowed",
                    e.getMessage());
        } finally
        {
            Assert.assertFalse("delete " + file.getAbsolutePath(), file.delete());
        }
    }

    @Test
    public void sysinFileout () throws Throwable
    {
        Helper.initializeFor("TEST sysinSysout");

        final List<String> out = new ArrayList<>();
        out.add("line 1");
        out.add("line 2");

        final StringBuilder sb = new StringBuilder();
        for (final String outline : out)
        {
            sb.append(outline).append(System.getProperty("line.separator"));
        }
        final InputStream inputStream = new StringBufferInputStream(sb.toString());
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();

        final FunnelContext context = Funnel.sort("-o" + file.getAbsolutePath() + " --max 2 -c original --eol CR,LF"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void variableSysinSysout () throws Throwable
    {
        Helper.initializeFor("TEST variableSysinSysout");

        final List<String> out = new ArrayList<>();
        out.add("line 1");
        out.add("line 2");

        final StringBuilder sb = new StringBuilder();
        for (final String outline : out)
        {
            sb.append(outline).append(System.getProperty("line.separator"));
        }
        final InputStream inputStream = new StringBufferInputStream(sb.toString());
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort("--max 2 -c original --eol cr,lf " + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }
}
