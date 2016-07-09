package com.obdobion.funnel;

import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.orderby.DateKey;
import com.obdobion.funnel.orderby.DisplayFloatKey;
import com.obdobion.funnel.orderby.DisplayIntKey;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyDirection;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.orderby.KeyType;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class FormatTests
{
    static private void verifyFormatted (final byte[] bb, final KeyContext kx)
    {
        for (int b = 0; b < bb.length; b++)
            Assert.assertEquals("formatted key " + b, bb[b], kx.key[b]);
    }

    @Test
    public void dateFormatInvalid () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final KeyPart key = KeyType.create(KeyType.Date.name());
        key.offset = 0;
        key.length = 20;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "yyyy-MM-dd";
        final KeyContext kx = Helper.dummyKeyContext(" 1960-04-a");
        try
        {
            key.pack(kx);
            verifyFormatted(new byte[]
            {
                -128,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            }, kx);
            Assert.fail("ParseException was expected");
        } catch (final Exception e)
        {
            Assert.assertEquals("Unparseable date: \"1960-04-a\"", e.getMessage());
        }

    }

    @Test
    public void dateFormatTrimLeft () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DateKey key = new DateKey();
        key.offset = 0;
        key.length = 20;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "yyyy-MM-dd";
        final KeyContext kx = Helper.dummyKeyContext(" 1960-04-09");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            127,
            -1,
            -1,
            -72,
            -126,
            -64,
            95,
            0
        }, kx);
    }

    @Test
    public void floatFormatTrimRight () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayFloatKey key = new DisplayFloatKey();
        key.offset = 0;
        key.length = 8;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "####.###";
        final KeyContext kx = Helper.dummyKeyContext("5.1  ");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            -64,
            20,
            102,
            102,
            102,
            102,
            102,
            102
        }, kx);
    }

    @Test
    public void floatNegDollarFormat () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayFloatKey key = new DisplayFloatKey();
        key.offset = 0;
        key.length = 12;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "####.##";
        final KeyContext kx = Helper.dummyKeyContext("$5,000.10-");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            63,
            76,
            119,
            -26,
            102,
            102,
            102,
            101
        }, kx);
    }

    @Test
    public void floatPosDollarFormat () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayFloatKey key = new DisplayFloatKey();
        key.offset = 0;
        key.length = 8;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "####.##";
        final KeyContext kx = Helper.dummyKeyContext("$5,000.10");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            -64,
            -77,
            -120,
            25,
            -103,
            -103,
            -103,
            -102
        }, kx);
    }

    @Test
    public void integerFormatTrimLeft () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext(" 5");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            -128,
            0,
            0,
            0,
            0,
            0,
            0,
            5
        }, kx);
    }

    @Test
    public void integerFormatTrimRight () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext("5   ");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            -128,
            0,
            0,
            0,
            0,
            0,
            0,
            5
        }, kx);
    }

    @Test
    public void integerNegDollarFormat () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 5;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext("$-5,0");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            127,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -50
        }, kx);
    }

    @Test
    public void integerNegFormatAASC () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.AASC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext("-5");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            -128,
            0,
            0,
            0,
            0,
            0,
            0,
            5
        }, kx);
    }

    @Test
    public void integerNegFormatADESC () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.ADESC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext("-5");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            127,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -5
        }, kx);
    }

    @Test
    public void integerNegFormatASC () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext("-5");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            127,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -5
        }, kx);
    }

    @Test
    public void integerNegFormatDESC () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.DESC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext("-5");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            -128,
            0,
            0,
            0,
            0,
            0,
            0,
            5
        }, kx);
    }

    @Test
    public void integerNegLeftFormat () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext(" -5");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            127,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -5
        }, kx);
    }

    @Test
    public void integerNegRightFormat () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext(" 5-");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            127,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -5
        }, kx);
    }

    @Test
    public void integerPosDollarFormat () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext("$5,0");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            -128,
            0,
            0,
            0,
            0,
            0,
            0,
            50
        }, kx);
    }

    @Test
    public void integerPosFormatAASC () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.AASC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext("5");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            -128,
            0,
            0,
            0,
            0,
            0,
            0,
            5
        }, kx);
    }

    @Test
    public void integerPosFormatADESC () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.ADESC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext("5");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            127,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -5
        }, kx);
    }

    @Test
    public void integerPosFormatASC () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.ASC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext("5");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            -128,
            0,
            0,
            0,
            0,
            0,
            0,
            5
        }, kx);
    }

    @Test
    public void integerPosFormatDESC () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final DisplayIntKey key = new DisplayIntKey();
        key.offset = 0;
        key.length = 4;
        key.direction = KeyDirection.DESC;
        key.parseFormat = "####";
        final KeyContext kx = Helper.dummyKeyContext("5");
        key.pack(kx);
        verifyFormatted(new byte[]
        {
            127,
            -1,
            -1,
            -1,
            -1,
            -1,
            -1,
            -5
        }, kx);
    }

    @Test
    public void lengthMaxString () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        in.add("row 1000");
        final File file = Helper.createUnsortedFile(testName, in);

        final FunnelContext context = Funnel
                .sort(Helper.config(), file.getAbsolutePath()
                    + " -r --row 130 --fixedIn 10 "
                    + "--col(-n col1 string -o0)"
                    + "--orderby(col1)");
        Assert.assertEquals("key length", 255, context.getKeys().get(0).length);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void lengthOverride () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        in.add("row 1000");
        final File file = Helper.createUnsortedFile(testName, in);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -r --row 130 --fixedIn 10"
            + " --col(--name col1 integer -o4 -l4 --format '###')"
            + " --orderby(col1)");
        Assert.assertEquals("key length", 4, context.getKeys().get(0).length);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void lengthUnspecified () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        in.add("row 1000");
        final File file = Helper.createUnsortedFile(testName, in);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -r --row 130 --fixedIn 10"
            + " --col(-n col1 integer -o4 --format '###')"
            + "--orderby(col1)");
        Assert.assertEquals("key length", 3, context.getKeys().get(0).length);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void misspelledCol () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        try
        {
            Funnel.sort(Helper.config(), " --col(-n col1 string) --format(colone)");
            Assert.fail("Expected an exception");
        } catch (final ParseException e)
        {
            Assert.assertEquals("--formatOut must be a defined column or header: colone", e.getMessage());
        }
    }

    @Test
    public void misspelledEqu () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final List<String> in = new ArrayList<>();
        in.add("row 1000");
        final File file = Helper.createUnsortedFile(testName, in);
        try
        {
            Funnel.sort(Helper.config(), file.getAbsolutePath()
                + " --col(-n col1 string) --format(-ecolone -l1 -d '%03d')");
            Assert.fail("Expected an exception");
        } catch (final Exception e)
        {
            Assert.assertEquals("invalid equation result for --format(colone)", e.getMessage());
        }
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }

    @Test
    public void offsetDefault () throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> in = new ArrayList<>();
        for (int r = 0; r < 130; r++)
        {
            in.add("row " + (r + 1000));
        }

        final File file = Helper.createUnsortedFile(testName, in);
        FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -r --row 130 --fixedIn 10"
            + " --col(-n col1 string)"
            + " --orderby(col1)");
        Assert.assertEquals("key offset", 0, context.getKeys().get(0).offset);

        context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + " -r --row 130 --fixedIn 10"
            + " --col(-n col1 -o1 string)"
            + " --orderby(col1)");
        Assert.assertEquals("key offset", 1, context.getKeys().get(0).offset);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());
    }
}