package com.obdobion;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;

import org.junit.Assert;

import com.obdobion.funnel.AppContext;
import com.obdobion.funnel.columns.OutputFormatHelper;
import com.obdobion.funnel.orderby.KeyContext;

/**
 * <p>
 * Helper class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class Helper
{

    static final File          workDir = new File("target");
    /** Constant <code>DEBUG="ON"</code> */
    public static final String DEBUG   = "ON";

    /**
     * <p>
     * compare.
     * </p>
     *
     * @param file a {@link java.io.File} object.
     * @param expectedLines a {@link java.util.List} object.
     * @throws java.io.IOException if any.
     */
    static public void compare(final File file, final List<String> expectedLines)
            throws IOException
    {
        try (final BufferedReader sorted = new BufferedReader(new FileReader(file)))
        {
            for (final String expected : expectedLines)
            {
                final String actual = sorted.readLine();
                Assert.assertEquals(expected, actual);
            }
        }
    }

    /**
     * <p>
     * compareFixed.
     * </p>
     *
     * @param file a {@link java.io.File} object.
     * @param expectedData a {@link java.util.List} object.
     * @throws java.io.IOException if any.
     */
    static public void compareFixed(final File file, final List<String> expectedData)
            throws IOException
    {
        try (final BufferedReader sorted = new BufferedReader(new FileReader(file)))
        {
            final char[] foundChar = new char[1];
            for (final String aRow : expectedData)
                for (final byte expected : aRow.getBytes())
                {
                    Assert.assertEquals("characters read", 1, sorted.read(foundChar));
                    Assert.assertEquals(expected, (byte) foundChar[0]);
                }
        }
    }

    /**
     * <p>
     * compareFixed.
     * </p>
     *
     * @param file a {@link java.io.File} object.
     * @param expectedData a {@link java.lang.String} object.
     * @throws java.io.IOException if any.
     */
    static public void compareFixed(final File file, final String expectedData)
            throws IOException
    {
        try (final BufferedReader sorted = new BufferedReader(new FileReader(file)))
        {
            final char[] foundChar = new char[1];
            for (final byte expected : expectedData.getBytes())
            {
                Assert.assertEquals("characters read", 1, sorted.read(foundChar));
                Assert.assertEquals(expected, (byte) foundChar[0]);
            }
        }
    }

    /**
     * <p>
     * config.
     * </p>
     *
     * @return a {@link com.obdobion.funnel.AppContext} object.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    static public AppContext config() throws IOException, ParseException
    {
        return new AppContext(System.getProperty("user.dir"));
    }

    /**
     * <p>
     * createFixedUnsortedFile.
     * </p>
     *
     * @param prefix a {@link java.lang.String} object.
     * @param lines a {@link java.util.List} object.
     * @param rowLength a int.
     * @return a {@link java.io.File} object.
     * @throws java.io.IOException if any.
     */
    static public File createFixedUnsortedFile(
            final String prefix,
            final List<String> lines,
            final int rowLength)
                    throws IOException
    {
        final File file = File.createTempFile(prefix + ".", ".in", workDir);
        try (final BufferedWriter out = new BufferedWriter(new FileWriter(file)))
        {
            for (int idx = 0; idx < lines.size(); idx++)
            {
                final String line = lines.get(idx);
                out.write(line);
                for (int fill = line.length(); fill < rowLength; fill++)
                    out.write(" ");
            }
        }
        return file;
    }

    /**
     * <p>
     * createUnsortedFile.
     * </p>
     *
     * @param lines a {@link java.util.List} object.
     * @return a {@link java.io.File} object.
     * @throws java.io.IOException if any.
     */
    static public File createUnsortedFile(final List<String> lines) throws IOException
    {
        return createUnsortedFile("funnel", lines, true);
    }

    /**
     * <p>
     * createUnsortedFile.
     * </p>
     *
     * @param prefix a {@link java.lang.String} object.
     * @param lines a {@link java.util.List} object.
     * @return a {@link java.io.File} object.
     * @throws java.io.IOException if any.
     */
    static public File createUnsortedFile(final String prefix, final List<String> lines) throws IOException
    {
        return createUnsortedFile(prefix, lines, true);
    }

    /**
     * <p>
     * createUnsortedFile.
     * </p>
     *
     * @param prefix a {@link java.lang.String} object.
     * @param lines a {@link java.util.List} object.
     * @param includeTrailingLineTerminator a boolean.
     * @return a {@link java.io.File} object.
     * @throws java.io.IOException if any.
     */
    static public File createUnsortedFile(
            final String prefix,
            final List<String> lines,
            final boolean includeTrailingLineTerminator)
                    throws IOException
    {
        final File file = File.createTempFile(prefix + ".", ".in", workDir);
        try (final BufferedWriter out = new BufferedWriter(new FileWriter(file)))
        {
            for (int idx = 0; idx < lines.size(); idx++)
            {
                final int lengthToWrite = OutputFormatHelper.lengthToWrite(lines.get(idx).getBytes(), 0,
                        lines.get(idx).length(), true);

                final String line = lines.get(idx);
                if (idx > 0)
                    out.newLine();
                out.write(line, 0, lengthToWrite);
            }
            if (includeTrailingLineTerminator)
                out.newLine();
        }
        return file;
    }

    /**
     * <p>
     * dummyKeyContext.
     * </p>
     *
     * @param rawBytes a {@link java.lang.String} object.
     * @return a {@link com.obdobion.funnel.orderby.KeyContext} object.
     */
    static public KeyContext dummyKeyContext(final String rawBytes)
    {
        final KeyContext kx = new KeyContext();
        kx.key = new byte[255];
        kx.keyLength = 0;
        kx.rawRecordBytes = new byte[1][];
        kx.rawRecordBytes[0] = rawBytes.getBytes();
        kx.recordNumber = 0;
        return kx;
    }

    /**
     * <p>
     * initializeFor.
     * </p>
     *
     * @param testCaseName a {@link java.lang.String} object.
     */
    public static void initializeFor(final String testCaseName)
    {
        System.out.println();
        System.out.println(testCaseName);
    }

    /**
     * <p>
     * myDataCSVFileName.
     * </p>
     *
     * @return a {@link java.lang.String} object.
     */
    public static String myDataCSVFileName()
    {
        return "./src/examples/data/MyDataCSV.in";
    }

    /**
     * <p>
     * outFile.
     * </p>
     *
     * @param testName a {@link java.lang.String} object.
     * @return a {@link java.io.File} object.
     */
    public static File outFile(final String testName)
    {
        return new File(outFileName(testName));
    }

    /**
     * <p>
     * outFileName.
     * </p>
     *
     * @param jUnitTestName a {@link java.lang.String} object.
     * @return a {@link java.lang.String} object.
     */
    public static String outFileName(final String jUnitTestName)
    {
        return workDir + "\\" + jUnitTestName + ".out";
    }

    /**
     * <p>
     * outFileWhenInIsSysin.
     * </p>
     *
     * @return a {@link java.io.File} object.
     * @throws java.io.IOException if any.
     */
    static public File outFileWhenInIsSysin() throws IOException
    {
        return File.createTempFile("funnel.", ".out", workDir);
    }

    /**
     * <p>
     * testName.
     * </p>
     *
     * @return a {@link java.lang.String} object.
     */
    static public String testName()
    {
        final StackTraceElement[] ste = Thread.currentThread().getStackTrace();
        return ste[2].getMethodName();
    }
}
