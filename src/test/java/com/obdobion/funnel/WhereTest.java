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
 * WhereTest class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class WhereTest
{

    /**
     * <p>
     * badDataOnFirstRow.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void badDataOnFirstRow()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("07/09/2010 10:59:47 07/09/2010 00:00:00 0080                                    "
                + "10B0000001023080400000QQO       Tumber Hull L lc            1519     M0000033333");

        final File file = Helper.createUnsortedFile(testName, in1, false);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --fixedIn 80"
                + " --columns"
                + " (-n typeCode         integer --offset 0   --length 1)"
                + " (-n typeStatus       String  --offset 1   --length 1)"
                + " (-n typeInitial      String  --offset 2   --length 1)"
                + " (-n lastModifiedTime integer --offset 3   --length 19)"
                + " (-n initials         String  --offset 22  --length 10)"
                + " (-n memberName       String  --offset 32  --length 28)"
                + " (-n accountType      String  --offset 60  --length 1)"
                + " (-n clearFirm        String  --offset 63  --length 3)"
                + " (-n mailBox          String  --offset 67  --length 4)"
                + " (-n recordType       String  --offset 68  --length 1)"
                + " (-n role             String  --offset 60  --length 1)"
                + " (-n membershipKey    Integer --offset 70  --length 10)"
                + " --where \"rtrim(initials) = 'QQO'\""
                + " -r ");

        Assert.assertEquals("records", 1L, context.getWriteCount());

        final List<String> exp = new ArrayList<>();
        exp.add("10B0000001023080400000QQO       Tumber Hull L lc            1519     M0000033333");
        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * badOrderByName.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void badOrderByName()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), "*"
                    + " --col(int -o0 -l5 -n field1)"
                    + " --col(int -o5 -l5 -n field2)"
                    + " --where '(field1 != field2)'"
                    + " --orderby(field1)(field3)");
            Assert.fail("should have failed");
        } catch (final ParseException e)
        {
            Assert.assertEquals("OrderBy must be a defined column: field3", e.getMessage());
        }
    }

    /**
     * <p>
     * columnDefinedWithinKey.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void columnDefinedWithinKey()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(testName, in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --where '(zipcode >= 50100 && zipcode <= 50200)'"
                + " --col(-n zipCode int -o0 -l5)"
                + "--orderby(zipcode desc)"
                + " -r ");

        Assert.assertEquals("records", 2L, context.getWriteCount());
        final List<String> exp = new ArrayList<>();
        exp.add("50200");
        exp.add("50100");
        Helper.compare(file, exp);
        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * fixedLength.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void fixedLength()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(testName, in1);
        final int rowSize = System.lineSeparator().length() + 5;

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --fixedIn " + rowSize
                + " --where 'zipcode = 50100'"
                + " --col(-n zipCode int -o0 -l5)"
                + " --orderby(zipcode asc)"
                + " -r ");

        Assert.assertEquals("records", 1L, context.getWriteCount());
        final List<String> exp = new ArrayList<>();
        exp.add("50100");
        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * fixedLengthSelectionOf2.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void fixedLengthSelectionOf2()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(testName, in1);
        final int rowSize = System.lineSeparator().length() + 5;

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --fixedIn " + rowSize + " --variableout CR LF"
                + " --where '(zipcode >= 50100 && zipcode <= 50200)'"
                + " --col(-n zipCode int -o0 -l5)"
                + " --format(zipcode)"
                + "--orderby(zipcode desc)"
                + " -r ");

        Assert.assertEquals("records", 2L, context.getWriteCount());
        final List<String> exp = new ArrayList<>();
        exp.add("50200");
        exp.add("50100");
        Helper.compare(file, exp);
        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * multiWhere.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void multiWhere()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(testName, in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)"
                + " --where 'zipcode > 50000' 'zipcode < 60000'"
                + " --orderby(zipCode asc)"
                + " -r ");

        Assert.assertEquals("records", 99L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * oneColumnWhere.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void oneColumnWhere()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(testName, in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)"
                + " --where 'zipcode = 50100'"
                + " --orderby(zipcode asc)"
                + " -r ");

        Assert.assertEquals("records", 1L, context.getWriteCount());
        final List<String> exp = new ArrayList<>();
        exp.add("50100");
        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * selectOddNumberedRows.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void selectOddNumberedRows()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(testName, in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --where 'recordnumber % 2 = 1'"
                + " -r ");

        Assert.assertEquals("records", 450L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * sortOnColumn.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void sortOnColumn()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(testName, in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)"
                + " --where '(zipcode > 50000 && zipcode < 60000)'"
                + " --orderby(zipCode asc)"
                + " -r ");

        Assert.assertEquals("records", 99L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    /**
     * <p>
     * whereIntEqualTo.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void whereIntEqualTo()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 20000; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(testName, in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --col(-n zipCode int -o0 -l5)"
                + " --where 'zipcode = 10100'"
                + " -r ");

        Assert.assertEquals("records", 1L, context.getWriteCount());

        Assert.assertTrue(file.delete());
    }
}
