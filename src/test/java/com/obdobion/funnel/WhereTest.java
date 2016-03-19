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
    public void oneColumnWhere ()
            throws Throwable
    {
        Helper.initializeFor("TEST oneColumnWhere");

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(in1);

        final FunnelContext context = Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)"
                + " --where '(zipcode > 50000 && zipcode < 60000)'"
                + " --key(int -o0 -l5 asc)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 99L, context.publisher.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void selectOddNumberedRows ()
            throws Throwable
    {
        Helper.initializeFor("TEST selectOddNumberedRows");

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(in1);

        final FunnelContext context = Funnel.sort(file.getAbsolutePath()
                + " --where 'recordnumber % 2 = 1'"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 450L, context.publisher.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void columnDefinedWithinKey ()
            throws Throwable
    {
        Helper.initializeFor("TEST columnDefinedWithinKey");

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(in1);

        final FunnelContext context = Funnel.sort(file.getAbsolutePath()
                + " --where '(zipcode > 50000 && zipcode < 60000)'"
                + " --key(-n zipCode int -o0 -l5 asc)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 99L, context.publisher.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void fixedLength ()
            throws Throwable
    {
        Helper.initializeFor("TEST fixedLength");

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(in1);

        final FunnelContext context = Funnel.sort(file.getAbsolutePath()
                + " --fixed 7 "
                + " --where '(zipcode > 50000 && zipcode < 60000)'"
                + " --key(-n zipCode int -o0 -l5 asc)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 99L, context.publisher.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void sortOnColumn ()
            throws Throwable
    {
        Helper.initializeFor("TEST sortOnColumn");

        final List<String> in1 = new ArrayList<>();
        for (int x = 10000; x <= 99999; x += 100)
        {
            in1.add("" + x);
        }

        final File file = Helper.createUnsortedFile(in1);

        final FunnelContext context = Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l5 -n zipCode)"
                + " --where '(zipcode > 50000 && zipcode < 60000)'"
                + " --orderby(zipCode asc)"
                + " -r "
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 99L, context.publisher.getWriteCount());

        Assert.assertTrue(file.delete());
    }

    @Test
    public void badOrderBySort ()
            throws Throwable
    {
        Helper.initializeFor("TEST badOrderBySort");
        try
        {
            Funnel.sort("*.notFound"
                    + " --col(int -o0 -l5 -n zipCode)"
                    + " --where '(zipcode > 50000 && zipcode < 60000)'"
                    + " --orderby(zipCode asc) --key(int -o0 -l5)"
                    + Helper.DEFAULT_OPTIONS);
            Assert.fail("should have failed");
        } catch (ParseException e)
        {
            Assert.assertEquals("--orderBy and --key can not be used in the same sort", e.getMessage());
        }
    }

    @Test
    public void badOrderByName ()
            throws Throwable
    {
        Helper.initializeFor("TEST badOrderBySort");
        try
        {
            Funnel.sort("*.notFound"
                    + " --col(int -o0 -l5 -n field1)"
                    + " --col(int -o5 -l5 -n field2)"
                    + " --where '(field1 != field2)'"
                    + " --orderby(field1)(field3)"
                    + Helper.DEFAULT_OPTIONS);
            Assert.fail("should have failed");
        } catch (ParseException e)
        {
            Assert.assertEquals("OrderBy must be a defined column: field3", e.getMessage());
        }
    }
}
