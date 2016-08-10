package com.obdobion.funnel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.StringBufferInputStream;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.provider.AbstractInputCache;

/**
 * <p>InputCacheTests class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
@SuppressWarnings("deprecation")
public class InputCacheTests
{
    static private FunnelContext createDummyContext(final InputStream in, final PrintStream out) throws Exception
    {
        System.setIn(in);
        System.setOut(out);
        return new FunnelContext(Helper.config(), "");
    }

    /**
     * <p>bufferPositioningLargeNumbers.</p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void bufferPositioningLargeNumbers() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final long[] bufStarts = {
                0,
                32768
        };

        Assert.assertEquals(0, AbstractInputCache.findBufferIndexForPosition(32767, bufStarts));
        Assert.assertEquals(1, AbstractInputCache.findBufferIndexForPosition(32768, bufStarts));
        Assert.assertEquals(1, AbstractInputCache.findBufferIndexForPosition(32846, bufStarts));
    }

    /**
     * <p>bufferPositioningNotPow2.</p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void bufferPositioningNotPow2() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final long[] bufStarts = {
                0,
                10,
                20,
                30
        };

        Assert.assertEquals(0, AbstractInputCache.findBufferIndexForPosition(0, bufStarts));
        Assert.assertEquals(0, AbstractInputCache.findBufferIndexForPosition(9, bufStarts));
        Assert.assertEquals(1, AbstractInputCache.findBufferIndexForPosition(10, bufStarts));
        Assert.assertEquals(1, AbstractInputCache.findBufferIndexForPosition(19, bufStarts));
        Assert.assertEquals(2, AbstractInputCache.findBufferIndexForPosition(20, bufStarts));
        Assert.assertEquals(2, AbstractInputCache.findBufferIndexForPosition(29, bufStarts));
        Assert.assertEquals(3, AbstractInputCache.findBufferIndexForPosition(30, bufStarts));
    }

    /**
     * <p>bufferPositioningPow2.</p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void bufferPositioningPow2() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final long[] bufStarts = {
                0,
                10
        };

        Assert.assertEquals(0, AbstractInputCache.findBufferIndexForPosition(0, bufStarts));
        Assert.assertEquals(0, AbstractInputCache.findBufferIndexForPosition(9, bufStarts));
        Assert.assertEquals(1, AbstractInputCache.findBufferIndexForPosition(10, bufStarts));
        Assert.assertEquals(1, AbstractInputCache.findBufferIndexForPosition(19, bufStarts));
        Assert.assertEquals(1, AbstractInputCache.findBufferIndexForPosition(20, bufStarts));
        Assert.assertEquals(1, AbstractInputCache.findBufferIndexForPosition(29, bufStarts));
        Assert.assertEquals(1, AbstractInputCache.findBufferIndexForPosition(30, bufStarts));
    }

    /**
     * <p>inputStreamWith2BuffersByArray.</p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void inputStreamWith2BuffersByArray() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final int minRow = 100000;
        final int maxRows = 104106;

        final StringBuilder sb = new StringBuilder();
        for (long num = minRow; num < maxRows; num++)
            sb.append(num).append(System.getProperty("line.separator"));
        final InputStream testStream = new StringBufferInputStream(sb.toString());
        final File file = Helper.outFileWhenInIsSysin();
        try (final PrintStream outputStream = new PrintStream(new FileOutputStream(file)))
        {
            final FunnelContext context = createDummyContext(testStream, outputStream);

            final byte[] testBytes = new byte[8];
            for (long num = minRow; num < maxRows; num++)
            {
                context.inputCache.read(context.inputFileIndex(), testBytes, (num - minRow) * 8, 8);
                Assert.assertEquals("" + num, new String(testBytes).trim());
            }
        }
        Assert.assertTrue(file.delete());
    }

    /**
     * <p>inputStreamWith2BuffersByByte.</p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void inputStreamWith2BuffersByByte() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final int minRow = 100000;
        final int maxRows = 104106;

        final StringBuilder sb = new StringBuilder();
        for (long num = minRow; num < maxRows; num++)
            sb.append(num).append(System.getProperty("line.separator"));
        final InputStream testStream = new StringBufferInputStream(sb.toString());
        final File file = Helper.outFileWhenInIsSysin();
        try (final PrintStream outputStream = new PrintStream(new FileOutputStream(file)))
        {
            final FunnelContext context = createDummyContext(testStream, outputStream);

            final byte[] testBytes = new byte[8];
            for (long num = minRow; num < maxRows; num++)
            {
                for (int b = 0; b < 8; b++)
                    testBytes[b] = context.inputCache.readNextByte();
                Assert.assertEquals("" + num, new String(testBytes).trim());
            }
        }
        Assert.assertTrue(file.delete());
    }

    /**
     * <p>sortWith2Buffers.</p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void sortWith2Buffers() throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final int minRow = 100000;
        final int maxRows = 104106;

        final StringBuilder sb = new StringBuilder();
        for (long num = maxRows - 1; num >= minRow; num--)
            sb.append(num).append(System.getProperty("line.separator"));
        System.setIn(new StringBufferInputStream(sb.toString()));
        final File file = Helper.outFileWhenInIsSysin();
        try (final PrintStream outputStream = new PrintStream(new FileOutputStream(file)))
        {
            System.setOut(outputStream);
            Funnel.sort(Helper.config());
            outputStream.flush();
        }
        try (final BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            String line;
            for (long num = minRow; num < maxRows; num++)
            {
                line = br.readLine();
                Assert.assertEquals("" + num, line);
            }
        }
        Assert.assertTrue(file.delete());
    }
}
