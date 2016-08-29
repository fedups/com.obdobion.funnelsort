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
 * <p>
 * HeaderTests class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class HeaderTests
{
    /**
     * <p>
     * addHeaderToFile.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void addHeaderToFile()
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
                + " --headerOut(-e'datefmt(\"20160609\", \"yyyyMMdd\")' -l 28 -d '%tF')");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        final List<String> expectedLines = new ArrayList<>();
        expectedLines.add("2016-06-09                  ");
        expectedLines.add("TEST20160608 2");
        expectedLines.add("TEST20160608 1");
        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * consumeHeader.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void consumeHeader()
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

    /**
     * <p>
     * duplicateNameWithColumn.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void duplicateNameWithColumn()
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
                    + "      (string -l4 -n TYPE)");
            Assert.fail("Expected exception");
        } catch (final ParseException e)
        {
            Assert.assertEquals("columnsIn must be unique from headerIn: TYPE", e.getMessage());
        }
    }

    /**
     * <p>
     * duplicateNameWithHeader.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void duplicateNameWithHeader()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), ""
                    + " --col(string -l4 -n type)"
                    + "      (date   -l8 -n DATE -d'yyyyMMdd')"
                    + "      (int    -l2 -n SEQ)"
                    + " --headerIn"
                    + "      (date   -l8 -n TYPE -d'yyyyMMdd')"
                    + "      (string -l4 -n TYPE)");
            Assert.fail("Expected exception");
        } catch (final ParseException e)
        {
            Assert.assertEquals("headerIn must be unique: TYPE", e.getMessage());
        }
    }

    /**
     * <p>
     * filler.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void filler()
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
                + " --headerOut(-eRUNTIME  -l 28 -d '%tF')");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        final List<String> expectedLines = new ArrayList<>();
        expectedLines.add("2016-06-01                  ");
        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * notValidForCSV.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void notValidForCSV()
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
                    + "      (string -l4 -n TYPE)");
            Assert.fail("Expected exception");
        } catch (final ParseException e)
        {
            Assert.assertEquals("headerIn not supported for csv files", e.getMessage());
        }
    }

    /**
     * <p>
     * useHeaderForStop.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void useHeaderForStop()
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

    /**
     * <p>
     * useHeaderForWhere.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void useHeaderForWhere()
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

    /**
     * <p>
     * writeEntireHeaderToOutput.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void writeEntireHeaderToOutput()
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

    /**
     * <p>
     * writeHeaderColumnToDetailRecordAsCol.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void writeHeaderColumnToDetailRecordAsCol()
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

    /**
     * <p>
     * writeHeaderColumnToDetailRecordAsEqu.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void writeHeaderColumnToDetailRecordAsEqu()
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

    /**
     * <p>
     * writeHeaderOutWithEqu.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void writeHeaderOutWithEqu()
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
                + " --headerOut(-eRUNDATE -l 28 -d '%tF')");

        Assert.assertEquals("records", 2L, context.getWriteCount());

        final List<String> expectedLines = new ArrayList<>();
        expectedLines.add("2016-06-08                  ");
        expectedLines.add("TEST20160608 2");
        expectedLines.add("TEST20160608 1");
        Helper.compare(file, expectedLines);
        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * writeMinimalHeaderToOutput.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void writeMinimalHeaderToOutput()
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

    /**
     * <p>
     * writeSpecificColumnsToHeaderOutput.
     * </p>
     *
     * @throws java.lang.Throwable
     *             if any.
     */
    @Test
    public void writeSpecificColumnsToHeaderOutput()
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
