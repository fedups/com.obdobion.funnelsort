package com.obdobion.funnel;

import java.text.ParseException;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.orderby.KeyType;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>CommandLineOptionsTest class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class CommandLineOptionsTest
{
    FunnelContext ctx;

    /**
     * -tBoolean -k cacheInput --var cacheInput --def false
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void defineCacheInput() throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        ctx = new FunnelContext(Helper.config());
        Assert.assertFalse("default cacheInput", ctx.isNoCacheInput());

        ctx = new FunnelContext(Helper.config(), "--nocache");
        Assert.assertTrue("cacheInput", ctx.isNoCacheInput());

        ctx = new FunnelContext(Helper.config(), "-!nocache");
        Assert.assertFalse("cacheInput", ctx.isNoCacheInput());
    }

    /**
     * <p>defineCacheWork.</p>
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void defineCacheWork() throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        ctx = new FunnelContext(Helper.config());
        Assert.assertFalse("default diskWork", ctx.isDiskWork());
        Assert.assertTrue("default isCacheWork", ctx.isCacheWork());

        ctx = new FunnelContext(Helper.config(), "--diskWork");
        Assert.assertTrue("diskWork", ctx.isDiskWork());
        Assert.assertFalse("isCacheWork", ctx.isCacheWork());

        ctx = new FunnelContext(Helper.config(), "-!diskWork");
        Assert.assertFalse("diskWork", ctx.isDiskWork());
        Assert.assertTrue("isCacheWork", ctx.isCacheWork());
    }

    /**
     * -tBegin -k columnsIn -m1 --var inputColumnDefs --factoryMethod
     * KeyType.create --factoryA --type
     *
     * String(AlphaKey.class), Integer(DisplayIntKey.class),
     * Float(DisplayFloatKey.class), BInteger(BinaryIntKey.class),
     * BFloat(BinaryFloatKey.class), Date(DateKey.class);
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void defineColumnsSubparser() throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        ctx = new FunnelContext(Helper.config(), "--col(String)");
        Assert.assertEquals("columns", 1, ctx.columnHelper.getNames().size());
        Assert.assertNull("column name", ctx.columnHelper.getNames().get(0));
        Assert.assertEquals("column type", KeyType.String, ctx.columnHelper.get(null).typeName);
        /*
         * Only one null named column
         */
        try
        {
            ctx = new FunnelContext(Helper.config(), "--col(string)(int)");
            Assert.fail("should have failed because of more than one unnamed column");
        } catch (final ParseException e)
        {
            Assert.assertEquals("Column already defined: null", e.getMessage());
        }
        ctx = new FunnelContext(Helper.config(), "--col(String)(-n myInt int)");
        Assert.assertEquals("columns", 2, ctx.columnHelper.getNames().size());
        Assert.assertNull("column name", ctx.columnHelper.getNames().get(0));
        Assert.assertEquals("column name", "myInt", ctx.columnHelper.getNames().get(1));
        Assert.assertEquals("column type", KeyType.String, ctx.columnHelper.get(null).typeName);
        Assert.assertEquals("column type", KeyType.Integer, ctx.columnHelper.get("myInt").typeName);
        /*
         * More verbose
         */
        ctx = new FunnelContext(Helper.config(), "--col(String) --col(-n myInt int)");
        Assert.assertEquals("columns", 2, ctx.columnHelper.getNames().size());
        Assert.assertNull("column name", ctx.columnHelper.getNames().get(0));
        Assert.assertEquals("column name", "myInt", ctx.columnHelper.getNames().get(1));
        Assert.assertEquals("column type", KeyType.String, ctx.columnHelper.get(null).typeName);
        Assert.assertEquals("column type", KeyType.Integer, ctx.columnHelper.get("myInt").typeName);
        /*
         * Binary Integer type
         */
        ctx = new FunnelContext(Helper.config(), "--col(bI)");
        Assert.assertEquals("column type", KeyType.BInteger, ctx.columnHelper.get(null).typeName);
        /*
         * Display Float type
         */
        ctx = new FunnelContext(Helper.config(), "--col(float)");
        Assert.assertEquals("column type", KeyType.Float, ctx.columnHelper.get(null).typeName);
        /*
         * Binary Float type
         */
        ctx = new FunnelContext(Helper.config(), "--col(bfloat)");
        Assert.assertEquals("column type", KeyType.BFloat, ctx.columnHelper.get(null).typeName);
        /*
         * Date
         */
        ctx = new FunnelContext(Helper.config(), "--col(Date)");
        Assert.assertEquals("column type", KeyType.Date, ctx.columnHelper.get(null).typeName);
        /*
         * csv field
         */
        ctx = new FunnelContext(Helper.config(), "--col(String -f1)");
        Assert.assertEquals("column as field", 0, ctx.columnHelper.get(null).csvFieldNumber);
        /*
         * offset
         */
        ctx = new FunnelContext(Helper.config(), "--col(String -o10)");
        Assert.assertEquals("offset", 10, ctx.columnHelper.get(null).offset);
        Assert.assertEquals("length", 255, ctx.columnHelper.get(null).length);
        /*
         * offset
         */
        ctx = new FunnelContext(Helper.config(), "--col(String -l5)");
        Assert.assertEquals("offset", 5, ctx.columnHelper.get(null).length);
        /*
         * offset
         */
        ctx = new FunnelContext(Helper.config(), "--col(Date -d'yyyyMM')");
        Assert.assertEquals("format", "yyyyMM", ctx.columnHelper.get(null).parseFormat);
    }

    /**
     * -tWildFile -k inputfilename --var inputFiles -m1 -p --case
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void defineInputFile() throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        /*
         * This will change if the number of class files changes.
         */
        ctx = new FunnelContext(Helper.config(), "**/main/**/funnel/*.java");
        Assert.assertEquals("file count", 6, ctx.getInputFiles().files().size());
        /*
         * This will change if the number of class files changes.
         */
        ctx = new FunnelContext(Helper.config(), "**/main/**/funnel/*.java", "**/main/**/segment/*.java");
        Assert.assertEquals("file count", 12, ctx.getInputFiles().files().size());
    }

    /**
     * <p>version.</p>
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void version() throws Exception
    {
        Assert.assertEquals("version", "JUNIT.TESTING", Helper.config().version);
    }
}
