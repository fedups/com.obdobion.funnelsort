package com.obdobion.funnel;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.orderby.DateKey;
import com.obdobion.funnel.orderby.DisplayFloatKey;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyDirection;
import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 * 
 */
public class CompareTests
{
    static private SourceProxyRecord dummySourceProxyRecord (final KeyContext kx1, final int keySize)
    {

        final SourceProxyRecord spr1 = SourceProxyRecord.getInstance(null);
        spr1.originalLocation = 0;
        spr1.originalRecordNumber = 0;
        spr1.originalSize = 0;
        spr1.size = keySize;
        spr1.sortKey = kx1.key;
        return spr1;
    }

    @Test
    public void compareDates () throws Throwable
    {
        Helper.initializeFor("TEST compareDates");

        final DateKey key1 = new DateKey();
        key1.offset = 0;
        key1.length = 20;
        key1.direction = KeyDirection.ASC;
        key1.parseFormat = "yyyy-MM-dd";
        final KeyContext kx1 = Helper.dummyKeyContext(" 1960-04-09");
        key1.pack(kx1);
        final SourceProxyRecord spr1 = dummySourceProxyRecord(kx1, 8);

        final DateKey key2 = new DateKey();
        key2.offset = 0;
        key2.length = 20;
        key2.direction = KeyDirection.ASC;
        key2.parseFormat = "yyyy-MM-dd";
        final KeyContext kx2 = Helper.dummyKeyContext(" 1960-04-10");
        key2.pack(kx2);
        final SourceProxyRecord spr2 = dummySourceProxyRecord(kx2, 8);

        Assert.assertEquals("", -1, spr1.compareTo(spr2));
    }

    @Test
    public void compareDatesShortFormat () throws Throwable
    {
        Helper.initializeFor("TEST compareDatesShortFormat");

        final DateKey key1 = new DateKey();
        key1.offset = 0;
        key1.length = 20;
        key1.direction = KeyDirection.ASC;
        key1.parseFormat = "y-M-d";
        final KeyContext kx1 = Helper.dummyKeyContext(" 1960-04-09");
        key1.pack(kx1);
        final SourceProxyRecord spr1 = dummySourceProxyRecord(kx1, 8);

        final DateKey key2 = new DateKey();
        key2.offset = 0;
        key2.length = 20;
        key2.direction = KeyDirection.ASC;
        key2.parseFormat = "y-M-d";
        final KeyContext kx2 = Helper.dummyKeyContext(" 1960-04-10");
        key2.pack(kx2);
        final SourceProxyRecord spr2 = dummySourceProxyRecord(kx2, 8);

        Assert.assertEquals("", -1, spr1.compareTo(spr2));
    }

    @Test
    public void compareDatesWrapping () throws Throwable
    {
        Helper.initializeFor("TEST compareDatesWrapping");

        final DateKey key1 = new DateKey();
        key1.offset = 0;
        key1.length = 20;
        key1.direction = KeyDirection.ASC;
        key1.parseFormat = "y-M-d";
        final KeyContext kx1 = Helper.dummyKeyContext(" 1962-04-9");
        key1.pack(kx1);
        final SourceProxyRecord spr1 = dummySourceProxyRecord(kx1, 8);

        final DateKey key2 = new DateKey();
        key2.offset = 0;
        key2.length = 20;
        key2.direction = KeyDirection.ASC;
        key2.parseFormat = "y-M-d";
        final KeyContext kx2 = Helper.dummyKeyContext(" 91-3-40");
        key2.pack(kx2);
        final SourceProxyRecord spr2 = dummySourceProxyRecord(kx2, 8);

        Assert.assertEquals("", -1, spr1.compareTo(spr2));
    }

    @Test
    public void compareDatesY2K () throws Throwable
    {
        Helper.initializeFor("TEST compareDatesShortFormat");

        final DateKey key1 = new DateKey();
        key1.offset = 0;
        key1.length = 20;
        key1.direction = KeyDirection.ASC;
        key1.parseFormat = "y-M-d";
        final KeyContext kx1 = Helper.dummyKeyContext(" 1962-04-9");
        key1.pack(kx1);
        final SourceProxyRecord spr1 = dummySourceProxyRecord(kx1, 8);

        final DateKey key2 = new DateKey();
        key2.offset = 0;
        key2.length = 20;
        key2.direction = KeyDirection.ASC;
        key2.parseFormat = "y-M-d";
        final KeyContext kx2 = Helper.dummyKeyContext(" 91-4-10");
        key2.pack(kx2);
        final SourceProxyRecord spr2 = dummySourceProxyRecord(kx2, 8);

        Assert.assertEquals("", -1, spr1.compareTo(spr2));
    }

    @Test
    public void compareDouble () throws Throwable
    {
        Helper.initializeFor("TEST compareDouble");

        final DisplayFloatKey key1 = new DisplayFloatKey();
        key1.offset = 0;
        key1.length = 20;
        key1.direction = KeyDirection.ASC;
        final KeyContext kx1 = Helper.dummyKeyContext("123.456");
        key1.pack(kx1);
        final SourceProxyRecord spr1 = dummySourceProxyRecord(kx1, 8);

        final DisplayFloatKey key2 = new DisplayFloatKey();
        key2.offset = 0;
        key2.length = 20;
        key2.direction = KeyDirection.ASC;
        final KeyContext kx2 = Helper.dummyKeyContext("123.5");
        key2.pack(kx2);
        final SourceProxyRecord spr2 = dummySourceProxyRecord(kx2, 8);

        Assert.assertEquals("", -1, spr1.compareTo(spr2));
    }

    @Test
    public void compareDoubleDec () throws Throwable
    {
        Helper.initializeFor("TEST compareDoubleDec");

        final DisplayFloatKey key1 = new DisplayFloatKey();
        key1.offset = 0;
        key1.length = 20;
        key1.direction = KeyDirection.DESC;
        final KeyContext kx1 = Helper.dummyKeyContext("123456789012345.11");
        key1.pack(kx1);
        final SourceProxyRecord spr1 = dummySourceProxyRecord(kx1, 8);

        final DisplayFloatKey key2 = new DisplayFloatKey();
        key2.offset = 0;
        key2.length = 20;
        key2.direction = KeyDirection.DESC;
        final KeyContext kx2 = Helper.dummyKeyContext("123456789012345.12");
        key2.pack(kx2);
        final SourceProxyRecord spr2 = dummySourceProxyRecord(kx2, 8);

        Assert.assertEquals("", 1, spr1.compareTo(spr2));
    }

    @Test
    public void compareDoubleMax16Whole () throws Throwable
    {
        Helper.initializeFor("TEST compareDoubleMax16Whole");

        final DisplayFloatKey key1 = new DisplayFloatKey();
        key1.offset = 0;
        key1.length = 20;
        key1.direction = KeyDirection.ASC;
        final KeyContext kx1 = Helper.dummyKeyContext("1234567890123450");
        key1.pack(kx1);
        final SourceProxyRecord spr1 = dummySourceProxyRecord(kx1, 8);

        final DisplayFloatKey key2 = new DisplayFloatKey();
        key2.offset = 0;
        key2.length = 20;
        key2.direction = KeyDirection.ASC;
        final KeyContext kx2 = Helper.dummyKeyContext("1234567890123451");
        key2.pack(kx2);
        final SourceProxyRecord spr2 = dummySourceProxyRecord(kx2, 8);

        Assert.assertEquals("", -1, spr1.compareTo(spr2));
    }

    @Test
    public void compareDoubleMax18Decimals () throws Throwable
    {
        Helper.initializeFor("TEST compareDoubleMax18Decimals");

        final DisplayFloatKey key1 = new DisplayFloatKey();
        key1.offset = 0;
        key1.length = 20;
        key1.direction = KeyDirection.ASC;
        final KeyContext kx1 = Helper.dummyKeyContext("0.123456789012345670");
        key1.pack(kx1);
        final SourceProxyRecord spr1 = dummySourceProxyRecord(kx1, 8);

        final DisplayFloatKey key2 = new DisplayFloatKey();
        key2.offset = 0;
        key2.length = 20;
        key2.direction = KeyDirection.ASC;
        final KeyContext kx2 = Helper.dummyKeyContext("0.123456789012345671");
        key2.pack(kx2);
        final SourceProxyRecord spr2 = dummySourceProxyRecord(kx2, 8);

        Assert.assertEquals("", -1, spr1.compareTo(spr2));
    }

    @Test
    public void compareDoubleMaxRatio17Total () throws Throwable
    {
        Helper.initializeFor("TEST compareDoubleMaxRatio17Total");

        final DisplayFloatKey key1 = new DisplayFloatKey();
        key1.offset = 0;
        key1.length = 20;
        key1.direction = KeyDirection.ASC;
        final KeyContext kx1 = Helper.dummyKeyContext("123456789012345.11");
        key1.pack(kx1);
        final SourceProxyRecord spr1 = dummySourceProxyRecord(kx1, 8);

        final DisplayFloatKey key2 = new DisplayFloatKey();
        key2.offset = 0;
        key2.length = 20;
        key2.direction = KeyDirection.ASC;
        final KeyContext kx2 = Helper.dummyKeyContext("123456789012345.12");
        key2.pack(kx2);
        final SourceProxyRecord spr2 = dummySourceProxyRecord(kx2, 8);

        Assert.assertEquals("", -1, spr1.compareTo(spr2));
    }
}