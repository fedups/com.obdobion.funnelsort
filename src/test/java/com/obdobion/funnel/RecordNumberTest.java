package com.obdobion.funnel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;

/**
 * @author Chris DeGreef
 *
 */
public class RecordNumberTest
{

    @Test
    public void aggregate ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        for (int x = 1; x <= 5; x += 1)
        {
            in.add("" + x);
        }
        final List<String> expectedLines = new ArrayList<>();
        expectedLines.add("15 15");

        final File file = Helper.createUnsortedFile(testName, in);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -r -c o --col(-n myRecordNumber -l 2 int)"
            + " --sum (-n sum1 myRecordNumber)"
            + " --sum (-n sum2 -e'recordnumber')"
            + " --form(-esum1  -l 3 -d '%2d')(-esum2  -l 3 -d '%2d')");

        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void multiFileInput ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        for (int x = 1; x <= 5; x += 1)
        {
            in.add("" + x);
        }

        final List<String> in2 = new ArrayList<>();
        for (int x = 6; x <= 9; x += 1)
        {
            in2.add("" + x);
        }
        final List<String> expectedLines = new ArrayList<>();
        for (int x = 1; x <= 9; x += 1)
        {
            expectedLines.add("" + x + " " + x);
        }

        final File file = Helper.createUnsortedFile(testName, in);
        final File file2 = Helper.createUnsortedFile(testName, in2);
        final File fileOut = Helper.outFile(testName);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " " + file2.getAbsolutePath() + " --noc "
            + " --out " + fileOut.getAbsolutePath()
            + " --col(-n myRecordNumber -l 2 int) --orderBy (myRecordNumber)"
            + " --form(-erecordNumber -l 2)(myRecordNumber)");

        Helper.compare(fileOut, expectedLines);
        Assert.assertTrue(file.delete());
        Assert.assertTrue(file2.delete());
        Assert.assertTrue(fileOut.delete());
    }

    @Test
    public void normalFixed ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        for (int x = 1; x <= 5; x += 1)
        {
            in.add("" + x);
        }
        final List<String> expectedLines = new ArrayList<>();
        for (int x = 1; x <= 5; x += 1)
        {
            expectedLines.add("" + x + " " + x);
        }

        final File file = Helper.createUnsortedFile(testName, in);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -r -c o --col(-n myRecordNumber -l 2 int) --fixedIn 3"
            + " --form(-erecordNumber -l 2)(myRecordNumber)");

        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void normalVariable ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        for (int x = 1; x <= 5; x += 1)
        {
            in.add("" + x);
        }
        final List<String> expectedLines = new ArrayList<>();
        for (int x = 1; x <= 5; x += 1)
        {
            expectedLines.add("" + x + " " + x);
        }

        final File file = Helper.createUnsortedFile(testName, in);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -r -c o --col(-n myRecordNumber -l 2 int)"
            + " --form(-erecordNumber -l 2)(myRecordNumber)");

        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void skip1Row ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        for (int x = 1; x <= 5; x += 1)
        {
            in.add("" + x);
        }
        final List<String> expectedLines = new ArrayList<>();
        for (int x = 2; x <= 5; x += 1)
        {
            expectedLines.add("" + x + " " + x);
        }

        final File file = Helper.createUnsortedFile(testName, in);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -r -c o --col(-n myRecordNumber -l 2 int)"
            + " --where 'recordNumber > 1'"
            + " --form(-erecordNumber -l 2)(myRecordNumber)");

        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void stopWhen ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        for (int x = 1; x <= 5; x += 1)
        {
            in.add("" + x);
        }
        final List<String> expectedLines = new ArrayList<>();
        for (int x = 1; x <= 2; x += 1)
        {
            expectedLines.add("" + x + " " + x);
        }

        final File file = Helper.createUnsortedFile(testName, in);

        Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -r -c o --col(-n myRecordNumber -l 2 int)"
            + " --stopWhen 'recordNumber = 3'"
            + " --form(-erecordNumber -l 2)(myRecordNumber)");

        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }
}
