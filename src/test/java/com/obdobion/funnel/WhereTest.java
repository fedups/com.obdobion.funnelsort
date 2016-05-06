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
public class WhereTest
{

    @Test
    public void badDataOnFirstRow ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("07/09/2010 10:59:47 07/09/2010 00:00:00 0080                                    "
            + "10B0000001023080400000QQO       Tumber Hull L lc            1519     M0000033333");

        final File file = Helper.createUnsortedFile("badDataOnFirstRow", in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --fixed 80"
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
            + " -r "
            + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1L, context.publisher.getWriteCount());

        final List<String> exp = new ArrayList<>();
        exp.add("10B0000001023080400000QQO       Tumber Hull L lc            1519     M0000033333");
        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void badOrderByName ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        try
        {
            Funnel.sort(Helper.config(), "*.notFound"
                + " --col(int -o0 -l5 -n field1)"
                + " --col(int -o5 -l5 -n field2)"
                + " --where '(field1 != field2)'"
                + " --orderby(field1)(field3)"
                + Helper.DEFAULT_OPTIONS);
            Assert.fail("should have failed");
        } catch (final ParseException e)
        {
            Assert.assertEquals("OrderBy must be a defined column: field3", e.getMessage());
        }
    }

    @Test
    public void columnDefinedWithinKey ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile("columnDefinedWithinKey", in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --where '(zipcode >= 50100 && zipcode <= 50200)'"
            + " --col(-n zipCode int -o0 -l5)"
            + "--orderby(zipcode desc)"
            + " -r "
            + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 2L, context.publisher.getWriteCount());
        final List<String> exp = new ArrayList<>();
        exp.add("50200");
        exp.add("50100");
        Helper.compare(file, exp);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void fixedLength ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile("fixedLength", in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --fixed 7 "
            + " --where 'zipcode = 50100'"
            + " --col(-n zipCode int -o0 -l5)"
            + " --orderby(zipcode asc)"
            + " -r "
            + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1L, context.publisher.getWriteCount());
        final List<String> exp = new ArrayList<>();
        exp.add("50100");
        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void fixedLengthSelectionOf2 ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile("fixedLengthSelectionOf2", in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -f7 --variableout"
            + " --where '(zipcode >= 50100 && zipcode <= 50200)'"
            + " --col(-n zipCode int -o0 -l5)"
            + " --format(zipcode)"
            + "--orderby(zipcode desc)"
            + " -r "
            + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 2L, context.publisher.getWriteCount());
        final List<String> exp = new ArrayList<>();
        exp.add("50200");
        exp.add("50100");
        Helper.compare(file, exp);
        Assert.assertTrue(file.delete());
    }

    @Test
    public void multiWhere ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile("sortOnColumn", in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)"
            + " --where 'zipcode > 50000' 'zipcode < 60000'"
            + " --orderby(zipCode asc)"
            + " -r "
            + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 99L, context.publisher.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void oneColumnWhere ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile("oneColumnWhere", in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)"
            + " --where 'zipcode = 50100'"
            + " --orderby(zipcode asc)"
            + " -r "
            + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1L, context.publisher.getWriteCount());
        final List<String> exp = new ArrayList<>();
        exp.add("50100");
        Helper.compare(file, exp);

        Assert.assertTrue(file.delete());
    }

    @Test
    public void selectOddNumberedRows ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile("selectOddNumberedRows", in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --where 'recordnumber % 2 = 1'"
            + " -r "
            + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 450L, context.publisher.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void sortOnColumn ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile("sortOnColumn", in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(int -o0 -l5 -n zipCode)"
            + " --where '(zipcode > 50000 && zipcode < 60000)'"
            + " --orderby(zipCode asc)"
            + " -r "
            + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 99L, context.publisher.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void whereIntEqualTo ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 20000; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile("whereIntEqualTo", in1);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " --col(-n zipCode int -o0 -l5)"
            + " --where 'zipcode = 10100'"
            + " -r "
            + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1L, context.publisher.getWriteCount());

        Assert.assertTrue(file.delete());
    }
}
