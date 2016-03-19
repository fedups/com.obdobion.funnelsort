package com.obdobion;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.junit.Assert;

import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class Helper
{

    static final File          workDir         = new File("/tmp");
    public static final String DEFAULT_OPTIONS = "";              // "--cachew--workDir "
                                                                   // + workDir;
    public static final String DEBUG           = "ON";

    static public void compare (final File file, final List<String> expectedLines)
        throws IOException
    {
        final BufferedReader sorted = new BufferedReader(new FileReader(file));
        for (final String expected : expectedLines)
        {
            final String actual = sorted.readLine();
            Assert.assertEquals(expected, actual);
        }
        sorted.close();
    }

    static public void compareFixed (final File file, final String expectedData)
        throws IOException
    {
        final BufferedReader sorted = new BufferedReader(new FileReader(file));
        final char[] foundChar = new char[1];
        for (final byte expected : expectedData.getBytes())
        {
            Assert.assertEquals("characters read", 1, sorted.read(foundChar));
            Assert.assertEquals(expected, (byte) foundChar[0]);
        }
        sorted.close();
    }

    static public File createUnsortedFile (final List<String> lines)
        throws IOException
    {
        return createUnsortedFile(lines, true);
    }

    static public File createUnsortedFile (final List<String> lines, final boolean includeTrailingLineTerminator)
        throws IOException
    {
        final File file = File.createTempFile("funnel.", ".in", workDir);
        final BufferedWriter out = new BufferedWriter(new FileWriter(file));
        for (int idx = 0; idx < lines.size(); idx++)
        {
            final String line = lines.get(idx);
            if (idx > 0)
                out.newLine();
            out.write(line);
        }
        if (includeTrailingLineTerminator)
            out.newLine();
        out.close();
        return file;
    }

    static public KeyContext dummyKeyContext (final String rawBytes)
    {
        final KeyContext kx = new KeyContext();
        kx.key = new byte[255];
        kx.keyLength = 0;
        kx.rawRecordBytes = new byte[1][];
        kx.rawRecordBytes[0] = rawBytes.getBytes();
        kx.recordNumber = 0;
        return kx;
    }

    public static void initializeFor (final String testCaseName)
    {
        System.out.println();
        System.out.println(testCaseName);
        System.setProperty(FunnelContext.SYSPARM_DEBUG, DEBUG);
        System.setProperty(FunnelContext.SYSPARM_VERSION, "TESTING");

    }

    static public File outFileWhenInIsSysin ()
        throws IOException
    {
        return File.createTempFile("funnel.", ".out", workDir);
    }
}
