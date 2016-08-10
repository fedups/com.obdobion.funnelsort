package com.obdobion.funnel;

import junit.framework.Assert;

import org.junit.Test;

import com.obdobion.Helper;

/**
 * <p>BinaryTest class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class BinaryTest
{
    static final int binaryfind (final long[] values, final int max, final long value)
    {
        int b = 0;
        int t = max;

        while (b < t)
        {

            final int m = (t + b) >>> 1;
            final long v = values[m];

            if (v == value)
                return m;

            if (value < v)
            {
                t = m;
                continue;
            }
            if (b == m)
                return b;
            b = m;
        }
        if (b == 0)
            return -1;
        return b;
    }

    static private int find (final long[] values, final int value)
    {
        final long loop = 10000000;

        final long start = System.currentTimeMillis();
        for (long x = 0; x < loop; x++)
            binaryfind(values, values.length, value);
        final long end = System.currentTimeMillis();

        System.out.println("value " + value + " " + (long) ((double) loop / (double) (end - start))
            + " / ms (15000 = best so far)");

        return binaryfind(values, values.length, value);
    }

    static private long[] values (final int count, final int incr)
    {
        final long[] values = new long[count];
        long current = 0;
        for (int x = 0; x < count; x++)
        {
            values[x] = current += incr;
        }
        return values;
    }

    /**
     * <p>binary0.</p>
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void binary0 () throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final long[] values = values(8000, 10);
        Assert.assertEquals(-1, find(values, 0));
    }

    /**
     * <p>binary10.</p>
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void binary10 () throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final long[] values = values(8000, 10);
        Assert.assertEquals(0, find(values, 10));

    }

    /**
     * <p>binary11.</p>
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void binary11 () throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final long[] values = values(8000, 10);
        Assert.assertEquals(0, find(values, 11));
    }

    /**
     * <p>binary20.</p>
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void binary20 () throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final long[] values = values(8000, 10);
        Assert.assertEquals(1, find(values, 20));
    }

    /**
     * <p>binary40000_direct_immediate_hit.</p>
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void binary40000_direct_immediate_hit () throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final long[] values = values(8000, 10);
        Assert.assertEquals(4000, find(values, 40010));

    }

    /**
     * <p>binary79999.</p>
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void binary79999 () throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final long[] values = values(8000, 10);
        Assert.assertEquals(7998, find(values, 79999));

    }

    /**
     * <p>binary80000.</p>
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void binary80000 () throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final long[] values = values(8000, 10);
        Assert.assertEquals(7999, find(values, 80000));
    }

    /**
     * <p>binary80001.</p>
     *
     * @throws java.lang.Exception if any.
     */
    @Test
    public void binary80001 () throws Exception
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final long[] values = values(1000000, 10);
        Assert.assertEquals(7999, find(values, 80001));
    }
}
