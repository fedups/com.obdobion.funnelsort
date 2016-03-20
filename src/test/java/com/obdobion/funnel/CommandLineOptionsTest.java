package com.obdobion.funnel;

import java.text.ParseException;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.funnel.parameters.FunnelContext;

public class CommandLineOptionsTest
{
    FunnelContext ctx;

    /**
     * -tBoolean -k cacheInput --var cacheInput --def false
     */
    @Test
    public void defineCacheInput () throws Exception
    {
        ctx = new FunnelContext();
        Assert.assertFalse("default cacheInput", ctx.cacheInput);

        ctx = new FunnelContext("--cacheI");
        Assert.assertTrue("cacheInput", ctx.cacheInput);

        ctx = new FunnelContext("-!cacheI");
        Assert.assertFalse("cacheInput", ctx.cacheInput);
    }

    /**
     * -tBoolean -k cacheWork --var cacheWork --def false
     */
    @Test
    public void defineCacheWork () throws Exception
    {
        ctx = new FunnelContext();
        Assert.assertFalse("default cacheWork", ctx.cacheWork);

        ctx = new FunnelContext("--cacheW");
        Assert.assertTrue("cacheWork", ctx.cacheWork);

        ctx = new FunnelContext("-!cacheW");
        Assert.assertFalse("cacheWork", ctx.cacheWork);
    }

    /**
     * -tBegin -k columnsIn -m1 --var inputColumnDefs --factoryMethod
     * KeyType.create --factoryA --type
     *
     * String(AlphaKey.class), Integer(DisplayIntKey.class),
     * Float(DisplayFloatKey.class), BInteger(BinaryIntKey.class),
     * BFloat(BinaryFloatKey.class), Date(DateKey.class);
     */
    @Test
    public void defineColumnsSubparser () throws Exception
    {
        ctx = new FunnelContext("--col(string)");
        Assert.assertEquals("columns", 1, ctx.columnHelper.getNames().size());
        Assert.assertNull("column name", ctx.columnHelper.getNames().get(0));
        Assert.assertEquals("column type", "String", ctx.columnHelper.get(null).typeName);
        /*
         * Only one null named column
         */
        try
        {
            ctx = new FunnelContext("--col(string)(int)");
            Assert.fail("should have failed because of more than one unnamed column");
        } catch (final ParseException e)
        {
            Assert.assertEquals("Column already defined: null", e.getMessage());
        }
        ctx = new FunnelContext("--col(string)(-n myInt int)");
        Assert.assertEquals("columns", 2, ctx.columnHelper.getNames().size());
        Assert.assertNull("column name", ctx.columnHelper.getNames().get(0));
        Assert.assertEquals("column name", "myInt", ctx.columnHelper.getNames().get(1));
        Assert.assertEquals("column type", "String", ctx.columnHelper.get(null).typeName);
        Assert.assertEquals("column type", "Integer", ctx.columnHelper.get("myInt").typeName);
        /*
         * More verbose
         */
        ctx = new FunnelContext("--col(string) --col(-n myInt int)");
        Assert.assertEquals("columns", 2, ctx.columnHelper.getNames().size());
        Assert.assertNull("column name", ctx.columnHelper.getNames().get(0));
        Assert.assertEquals("column name", "myInt", ctx.columnHelper.getNames().get(1));
        Assert.assertEquals("column type", "String", ctx.columnHelper.get(null).typeName);
        Assert.assertEquals("column type", "Integer", ctx.columnHelper.get("myInt").typeName);
        /*
         * Binary Integer type
         */
        ctx = new FunnelContext("--col(bI)");
        Assert.assertEquals("column type", "BInteger", ctx.columnHelper.get(null).typeName);
        /*
         * Display Float type
         */
        ctx = new FunnelContext("--col(float)");
        Assert.assertEquals("column type", "Float", ctx.columnHelper.get(null).typeName);
        /*
         * Binary Float type
         */
        ctx = new FunnelContext("--col(bfloat)");
        Assert.assertEquals("column type", "BFloat", ctx.columnHelper.get(null).typeName);
        /*
         * Date
         */
        ctx = new FunnelContext("--col(Date)");
        Assert.assertEquals("column type", "Date", ctx.columnHelper.get(null).typeName);
        /*
         * csv field
         */
        ctx = new FunnelContext("--col(String -f1)");
        Assert.assertEquals("column as field", 1, ctx.columnHelper.get(null).csvFieldNumber);
        /*
         * offset
         */
        ctx = new FunnelContext("--col(String -o10)");
        Assert.assertEquals("offset", 10, ctx.columnHelper.get(null).offset);
        Assert.assertEquals("length", 255, ctx.columnHelper.get(null).length);
        /*
         * offset
         */
        ctx = new FunnelContext("--col(String -l5)");
        Assert.assertEquals("offset", 5, ctx.columnHelper.get(null).length);
        /*
         * offset
         */
        ctx = new FunnelContext("--col(Date -d'yyyyMM')");
        Assert.assertEquals("format", "yyyyMM", ctx.columnHelper.get(null).parseFormat);
    }

    /**
     * -tWildFile -k inputfilename --var inputFiles -m1 -p --case
     *
     * @throws Exception
     */
    @Test
    public void defineInputFile () throws Exception
    {
        /*
         * This will change if the number of class files changes.
         */
        ctx = new FunnelContext("**/main/**/funnel/*.java");
        Assert.assertEquals("file count", 5, ctx.inputFiles.files().size());
        /*
         * This will change if the number of class files changes.
         */
        ctx = new FunnelContext("**/main/**/funnel/*.java", "**/main/**/segment/*.java");
        Assert.assertEquals("file count", 11, ctx.inputFiles.files().size());
    }

    @Test
    public void defineInputFileExact () throws Exception
    {
        ctx = new FunnelContext("C:/tmp/funnel.3339840198501934555.in -r ");
    }

    @Test
    public void version () throws Exception
    {
        System.setProperty("version", "1.2.3");
        ctx = new FunnelContext("--version");
        Assert.assertEquals("version", "1.2.3", ctx.getVersion());
    }
}
