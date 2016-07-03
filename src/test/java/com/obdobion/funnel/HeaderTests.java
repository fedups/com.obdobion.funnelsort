package com.obdobion.funnel;

import java.io.File;
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
public class HeaderTests
{
    @Test
    public void addHeaderToFile ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("TEST20160608 1");
        logFile.add("TEST20160608 2");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --replace"
            + " --col(string -l4 -n TYPE)"
            + "      (date   -l8 -n DATE -d'yyyyMMdd')"
            + "      (int    -l2 -n SEQ)"
            + " --orderBy(seq desc)"
            + " --headerOut(-e'date(\"20160609\", \"yyyyMMdd\")' -l 28 -d '%tc')");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        final List<String> expectedLines = new ArrayList<>();
        expectedLines.add("Thu Jun 09 00:00:00 CDT 2016");
        expectedLines.add("TEST20160608 2");
        expectedLines.add("TEST20160608 1");
        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void consumeHeader ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("20160608TEST");
        logFile.add("TEST20160608 1");
        logFile.add("TEST20160608 2");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(string -l4 -n TYPE)"
            + "      (date   -l8 -n DATE -d'yyyyMMdd')"
            + "      (int    -l2 -n SEQ)"
            + " --headerIn"
            + "      (date   -l8 -n RUNDATE -d'yyyyMMdd')"
            + "      (string -l4 -n RUNTYPE)"
            + " --headerOut()");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void duplicateNameWithColumn ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), ""
                + " --col(string -l4 -n TYPE)"
                + "      (date   -l8 -n DATE -d'yyyyMMdd')"
                + "      (int    -l2 -n SEQ)"
                + " --headerIn"
                + "      (date   -l8 -n DATE -d'yyyyMMdd')"
                + "      (string -l4 -n TYPE)"
                    );
            Assert.fail("Expected exception");
        } catch (final ParseException e)
        {
            Assert.assertEquals("columnsIn must be unique from headerIn: TYPE", e.getMessage());
        }
    }

    @Test
    public void duplicateNameWithHeader ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), ""
                + " --col(string -l4 -n TYPE)"
                + "      (date   -l8 -n DATE -d'yyyyMMdd')"
                + "      (int    -l2 -n SEQ)"
                + " --headerIn"
                + "      (date   -l8 -n TYPE -d'yyyyMMdd')"
                + "      (string -l4 -n TYPE)"
                    );
            Assert.fail("Expected exception");
        } catch (final ParseException e)
        {
            Assert.assertEquals("headerIn must be unique: TYPE", e.getMessage());
        }
    }

    @Test
    public void filler ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("06/01/2016 16:43:56           06/01/2016 00:00:00 0230");
        logFile.add("TEST20160608 1");
        logFile.add("TEST20160608 2");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --replace"
            + " --headerIn "
            + "      (Date -n RUNTIME -d'MM/dd/yyyy HH:mm:ss')"
            + "      (filler -l 11)"
            + "      (Date -n BUSDATE -d'MM/dd/yyyy HH:mm:ss')"
            + "      (filler -l 1)"
            + "      (Int  -n LRECL -l4)"
            + " --headerOut(-eRUNTIME  -l 28 -d '%tc')"
                );

        Assert.assertEquals("records", 2L, context.getWriteCount());

        final List<String> expectedLines = new ArrayList<>();
        expectedLines.add("Wed Jun 01 16:43:56 CDT 2016");
        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void notValidForCSV ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), "--csv()"
                + " --col"
                + "      (int    -l2 -n SEQ)"
                + " --headerIn"
                + "      (date   -l8 -n DATE -d'yyyyMMdd')"
                + "      (string -l4 -n TYPE)"
                    );
            Assert.fail("Expected exception");
        } catch (final ParseException e)
        {
            Assert.assertEquals("headerIn not supported for csv files", e.getMessage());
        }
    }

    @Test
    public void useHeaderForStop ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("20160608TEST");
        logFile.add("TEST20160608 1");
        logFile.add("TEST20160608 2");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(string -l4 -n TYPE)"
            + "      (date   -l8 -n DATE -d'yyyyMMdd')"
            + "      (int    -l2 -n SEQ)"
            + " --headerIn"
            + "      (date   -l8 -n RUNDATE -d'yyyyMMdd')"
            + "      (string -l4 -n RUNTYPE)"
            + " --headerOut()"
            + " --stop 'runtype = type'");

        Assert.assertEquals("records", 0L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void useHeaderForWhere ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("20160608TEST");
        logFile.add("TEST20160608 1");
        logFile.add("TEST20160608 2");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(string -l4 -n TYPE)"
            + "      (date   -l8 -n DATE -d'yyyyMMdd')"
            + "      (int    -l2 -n SEQ)"
            + " --headerIn"
            + "      (date   -l8 -n RUNDATE -d'yyyyMMdd')"
            + "      (string -l4 -n RUNTYPE)"
            + " --headerOut()"
            + " --where 'runtype = type'");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void writeEntireHeaderToOutput ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("20160608TEST");
        logFile.add("TEST20160608 1");
        logFile.add("TEST20160608 2");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --replace"
            + " --col(string -l4 -n TYPE)"
            + "      (date   -l8 -n DATE -d'yyyyMMdd')"
            + "      (int    -l2 -n SEQ)"
            + " --orderBy(seq desc)"
            + " --headerIn"
            + "      (date   -l8 -n RUNDATE -d'yyyyMMdd')"
            + "      (string -l4 -n RUNTYPE)");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        final List<String> expectedLines = new ArrayList<>();
        expectedLines.add("20160608TEST");
        expectedLines.add("TEST20160608 2");
        expectedLines.add("TEST20160608 1");
        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void writeHeaderColumnToDetailRecordAsCol ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("20160608TEST");
        logFile.add("TEST20160608 1");
        logFile.add("TEST20160608 2");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath() + " --replace"
            + " --col(string -l4 -n TYPE)"
            + "      (date   -l8 -n DATE -d'yyyyMMdd')"
            + "      (int    -l2 -n SEQ)"
            + " --headerIn"
            + "      (date   -l8 -n RUNDATE -d'yyyyMMdd')"
            + "      (string -l4 -n RUNTYPE)"
            + " --headerOut()"
            + " --formatOut(TYPE)(RUNTYPE)(DATE)(RUNDATE)(SEQ)");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        final List<String> expectedLines = new ArrayList<>();
        expectedLines.add("TESTTEST2016060820160608 1");
        expectedLines.add("TESTTEST2016060820160608 2");
        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void writeHeaderColumnToDetailRecordAsEqu ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("20160608TEST");
        logFile.add("TEST20160608 1");
        logFile.add("TEST20160608 2");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(string -l4 -n TYPE)"
            + "      (date   -l8 -n DATE -d'yyyyMMdd')"
            + "      (int    -l2 -n SEQ)"
            + " --headerIn"
            + "      (date   -l8 -n RUNDATE -d'yyyyMMdd')"
            + "      (string -l4 -n RUNTYPE)"
            + " --headerOut()"
            + " --formatOut(TYPE)(-eRUNTYPE -l4)(DATE)(-eRUNDATE -l8 -d'%tY%1$tm%1$td')(SEQ)");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void writeHeaderOutWithEqu ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("20160608TEST");
        logFile.add("TEST20160608 1");
        logFile.add("TEST20160608 2");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --replace"
            + " --col(string -l4 -n TYPE)"
            + "      (date   -l8 -n DATE -d'yyyyMMdd')"
            + "      (int    -l2 -n SEQ)"
            + " --orderBy(seq desc)"
            + " --headerIn"
            + "      (date   -l8 -n RUNDATE -d'yyyyMMdd')"
            + "      (string -l4 -n RUNTYPE)"
            + " --headerOut(-eRUNDATE -l 28 -d '%tc')");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        final List<String> expectedLines = new ArrayList<>();
        expectedLines.add("Wed Jun 08 00:00:00 CDT 2016");
        expectedLines.add("TEST20160608 2");
        expectedLines.add("TEST20160608 1");
        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void writeMinimalHeaderToOutput ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("20160608TEST");
        logFile.add("TEST20160608 1");
        logFile.add("TEST20160608 2");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --replace"
            + " --col(string -l4 -n TYPE)"
            + "      (date   -l8 -n DATE -d'yyyyMMdd')"
            + "      (int    -l2 -n SEQ)"
            + " --orderBy(seq desc)"
            + " --headerIn(filler)");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        final List<String> expectedLines = new ArrayList<>();
        expectedLines.add("20160608TEST");
        expectedLines.add("TEST20160608 2");
        expectedLines.add("TEST20160608 1");
        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void writeSpecificColumnsToHeaderOutput ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> logFile = new ArrayList<>();
        logFile.add("20160608TEST");
        logFile.add("TEST20160608 1");
        logFile.add("TEST20160608 2");

        final File file = Helper.createUnsortedFile(testName, logFile);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --replace"
            + " --col(string -l4 -n TYPE)"
            + "      (date   -l8 -n DATE -d'yyyyMMdd')"
            + "      (int    -l2 -n SEQ)"
            + " --orderBy(seq desc)"
            + " --headerIn"
            + "      (date   -l8 -n RUNDATE -d'yyyyMMdd')"
            + "      (string -l4 -n RUNTYPE)"
            + " --headerOut(RUNTYPE)(RUNDATE)");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        final List<String> expectedLines = new ArrayList<>();
        expectedLines.add("TEST20160608");
        expectedLines.add("TEST20160608 2");
        expectedLines.add("TEST20160608 1");
        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }
}