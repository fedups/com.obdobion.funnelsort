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
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("line 1");
        out.add("line 2");

        final StringBuilder sb = new StringBuilder();
        for (final String outline : out)
        {
            sb.append(outline).append(System.getProperty("line.separator"));
        }

        final File file = Helper.createUnsortedFile(testName, out);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --replace --max 2 -c original --eol CR LF --eolOut LF");

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        file.delete();
    }

    @Test
    public void emptySysin () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final String out = "";

        final InputStream inputStream = new StringBufferInputStream(out);
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort(Helper.config(), "--fixedIn 10 --max 2 ");
        Assert.assertEquals("records", 0L, context.provider.actualNumberOfRows());
        outputStream.close();
        try
        {
            file.delete();
        } catch (final Exception e)
        {//
        }
    }

    @Test
    public void emptySysinNoMax () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final String out = "";

        final InputStream inputStream = new StringBufferInputStream(out);
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort(Helper.config(), "");
        Assert.assertEquals("records", 0L, context.provider.actualNumberOfRows());
        outputStream.close();
        try
        {
            file.delete();
        } catch (final Exception e)
        {//
        }
    }

    @Test
    public void everythingDefaults () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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
        @SuppressWarnings("resource")
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort(Helper.config());

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void fixedSysinSysout () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final String out = "line 1line 2";

        final InputStream inputStream = new StringBufferInputStream(out);
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        @SuppressWarnings("resource")
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        final FunnelContext context = Funnel.sort(Helper.config(), "--fixedIn 6--max 2 -c original");
        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compareFixed(file, out);
        try
        {
            outputStream.close();
            Assert.assertTrue(file.delete());
        } catch (final Exception e)
        {//
        }
    }

    @Test
    public void fixInFixOut () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("10       x");
        out.add("5   X     ");
        out.add("8      X  ");

        final File file = Helper.createFixedUnsortedFile(testName, out, 10);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --fixedIn 10 -r -c original");

        Assert.assertEquals("records", 3L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records", 3L, context.publisher.getWriteCount());
        Helper.compareFixed(file, out);
        Assert.assertTrue(file.delete());

    }

    @Test
    public void fixInFixOutLong () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("10       x");
        out.add("5   X     ");
        out.add("8      X  ");

        final File file = Helper.createFixedUnsortedFile(testName, out, 10);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --fixedIn 10 --fixedOut 20 -c original");

        Assert.assertEquals("records", 3L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records", 3L, context.publisher.getWriteCount());
        Helper.compareFixed(file, out);
        Assert.assertTrue(file.delete());

    }

    @Test
    public void fixInFixOutShort () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("10       x");
        out.add("5   X     ");
        out.add("8      X  ");

        final File file = Helper.createFixedUnsortedFile(testName, out, 10);

        out.clear();
        out.add("10   ");
        out.add("5   X");
        out.add("8    ");

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --fixedIn 10 --fixedOut 5 -r -c original");

        Assert.assertEquals("records", 3L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records", 3L, context.publisher.getWriteCount());
        Helper.compareFixed(file, out);
        Assert.assertTrue(file.delete());

    }

    @Test
    public void fixInVarOut () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("10       x");
        out.add("5   X");
        out.add("8      X");

        final File file = Helper.createFixedUnsortedFile(testName, out, 10);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --fixedIn 10 --variableOut -r -c original");

        Assert.assertEquals("records", 3L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records", 3L, context.publisher.getWriteCount());
        Helper.compare(file, out);
        Assert.assertTrue(file.delete());

    }

    @Test
    public void replaceErrorOut () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final String out = "line 1line 2";

        final InputStream inputStream = new StringBufferInputStream(out);
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        try
        {
            Funnel.sort(Helper.config(), "--replace --fixedIn 6--max 2 --col(S -nS) --o(S)");
            Assert.fail("Expected error");
        } catch (final ParseException e)
        {
            Assert.assertEquals(
                "error msg",
                "--replace requires --inputFile, redirection or piped input is not allowed",
                e.getMessage());
        } finally
        {
            outputStream.close();
            try
            {
                file.delete();
            } catch (final Exception e)
            {//
            }
        }
    }

    @Test
    public void replaceInput () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("line 1");
        out.add("line 2");

        final StringBuilder sb = new StringBuilder();
        for (final String outline : out)
        {
            sb.append(outline).append(System.getProperty("line.separator"));
        }

        final File file = Helper.createUnsortedFile(testName, out);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --replace --max 2 -c original --eol CR LF");

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void replacePipedInputNotAllowed () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final String out = "line 1line 2";

        final InputStream inputStream = new StringBufferInputStream(out);
        System.setIn(inputStream);

        final File file = Helper.outFileWhenInIsSysin();
        final PrintStream outputStream = new PrintStream(new FileOutputStream(file));
        System.setOut(outputStream);

        try
        {
            Funnel.sort(Helper.config(), "--replace --fixedIn 6--max 2 --col(-nc String) --o(c)");
            Assert.fail("Expected error");
        } catch (final ParseException e)
        {
            Assert.assertEquals(
                "error msg",
                "--replace requires --inputFile, redirection or piped input is not allowed",
                e.getMessage());
        } finally
        {
            outputStream.close();
            try
            {
                file.delete();
            } catch (final Exception e)
            {//
            }

        }
    }

    @Test
    public void sysinFileout () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final FunnelContext context = Funnel.sort(Helper.config(), "-o" + file.getAbsolutePath()
            + " --max 2 -c original --eol CR,LF");

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void variableSysinSysout () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

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

        final FunnelContext context = Funnel.sort(Helper.config(), "--max 2 -c original --eol cr,lf ");

        Assert.assertEquals("records", 2L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        outputStream.close();
        try
        {
            file.delete();
        } catch (final Exception e)
        {//
        }

    }

    @Test
    public void variableUnterminatedLastLine ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("line 1");
        out.add("line 2");
        out.add("unterminated");

        final File file = Helper.createUnsortedFile(testName, out, false);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + "--max 2 -c original --eol cr,lf -r ");

        Assert.assertEquals("records", 3L, context.provider.actualNumberOfRows());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void varInFixOut () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("10       X");
        out.add("5   X     ");
        out.add("8      X  ");

        final File file = Helper.createUnsortedFile(testName, out);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --fixedOut 10 -r -c original");

        Assert.assertEquals("records", 3L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records", 3L, context.publisher.getWriteCount());
        Helper.compareFixed(file, out);
        Assert.assertTrue(file.delete());

    }

    @Test
    public void varInVarOut () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("10       X");
        out.add("5   X");
        out.add("8      X");

        final File file = Helper.createUnsortedFile(testName, out, true);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -r -c original");

        Assert.assertEquals("records", 3L, context.provider.actualNumberOfRows());
        Assert.assertEquals("records", 3L, context.publisher.getWriteCount());
        Helper.compare(file, out);
        Assert.assertTrue(file.delete());
    }
}
