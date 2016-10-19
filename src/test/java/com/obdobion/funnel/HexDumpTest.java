package com.obdobion.funnel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * HexDumpTest class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class HexDumpTest
{

    /**
     * <p>
     * dumpColumnsWithFormat.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void dumpColumnsWithFormat()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("Data 123 Row number 1");
        in1.add("Data 4   Row number 2");
        in1.add("Data 78  Row number 3");

        final File file1 = Helper.createUnsortedFile(testName, Helper.DOS_EOL, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o " + output.getAbsolutePath()
                + " --VI CR,LF "
                + " --orderby(data)"
                + " --col(String -l 9 -ndata )(String -n title)"
                + " --format(title)(data)"
                + " --hexDump(data)(title)");

        Assert.assertEquals("records", 3L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();

        expectedOut.add("Row number 1Data 123");
        expectedOut.add("# 1 @ 0 for 21");
        expectedOut.add("data");
        expectedOut.add("0000  44617461 20313233                    |Data 123        |");
        expectedOut.add("title");
        expectedOut.add("0009  526F7720 6E756D62 65722031           |Row number 1    |");
        expectedOut.add("");
        expectedOut.add("Row number 2Data 4");
        expectedOut.add("# 2 @ 23 for 21");
        expectedOut.add("data");
        expectedOut.add("0000  44617461 2034                        |Data 4          |");
        expectedOut.add("title");
        expectedOut.add("0009  526F7720 6E756D62 65722032           |Row number 2    |");
        expectedOut.add("");
        expectedOut.add("Row number 3Data 78");
        expectedOut.add("# 3 @ 46 for 21");
        expectedOut.add("data");
        expectedOut.add("0000  44617461 203738                      |Data 78         |");
        expectedOut.add("title");
        expectedOut.add("0009  526F7720 6E756D62 65722033           |Row number 3    |");
        expectedOut.add("");

        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * dumpColumnWithoutFormat.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void dumpColumnWithoutFormat()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("This is row number 1.");
        in1.add("The second record in the file is this one.");
        in1.add("The third record is the longest and should cause more than one line on output.");

        final File file1 = Helper.createUnsortedFile(testName, Helper.DOS_EOL, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o " + output.getAbsolutePath()
                + " --VI CR,LF "
                + " -co"
                + " --col(string -n sentence)"
                + " --hexDump(sentence)");

        Assert.assertEquals("records", 3L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();

        expectedOut.add("# 1 @ 0 for 21");
        expectedOut.add("sentence");
        expectedOut.add("0000  54686973 20697320 726F7720 6E756D62  |This is row numb|");
        expectedOut.add("0016  65722031 2E                          |er 1.           |");
        expectedOut.add("");
        expectedOut.add("# 2 @ 23 for 42");
        expectedOut.add("sentence");
        expectedOut.add("0000  54686520 7365636F 6E642072 65636F72  |The second recor|");
        expectedOut.add("0016  6420696E 20746865 2066696C 65206973  |d in the file is|");
        expectedOut.add("0032  20746869 73206F6E 652E               | this one.      |");
        expectedOut.add("");
        expectedOut.add("# 3 @ 67 for 78");
        expectedOut.add("sentence");
        expectedOut.add("0000  54686520 74686972 64207265 636F7264  |The third record|");
        expectedOut.add("0016  20697320 74686520 6C6F6E67 65737420  | is the longest |");
        expectedOut.add("0032  616E6420 73686F75 6C642063 61757365  |and should cause|");
        expectedOut.add("0048  206D6F72 65207468 616E206F 6E65206C  | more than one l|");
        expectedOut.add("0064  696E6520 6F6E206F 75747075 742E      |ine on output.  |");
        expectedOut.add("");
        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>
     * dumpWithoutAnyColumnsDefined.
     * </p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void dumpWithoutAnyColumnsDefined()
            throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> in1 = new ArrayList<>();
        in1.add("This is row number 1.");
        in1.add("The second record in the file is this one.");
        in1.add("The third record is the longest and should cause more than one line on output.");

        final File file1 = Helper.createUnsortedFile(testName, Helper.DOS_EOL, in1);
        final FunnelContext context = Funnel.sort(Helper.config(), file1.getAbsolutePath()
                + " -o " + output.getAbsolutePath()
                + " --VI CR,LF "
                + " --hexDump()");

        Assert.assertEquals("records", 3L, context.getWriteCount());

        final List<String> expectedOut = new ArrayList<>();

        expectedOut.add("# 2 @ 23 for 42");
        expectedOut.add("0000  54686520 7365636F 6E642072 65636F72  |The second recor|");
        expectedOut.add("0016  6420696E 20746865 2066696C 65206973  |d in the file is|");
        expectedOut.add("0032  20746869 73206F6E 652E               | this one.      |");
        expectedOut.add("");
        expectedOut.add("# 3 @ 67 for 78");
        expectedOut.add("0000  54686520 74686972 64207265 636F7264  |The third record|");
        expectedOut.add("0016  20697320 74686520 6C6F6E67 65737420  | is the longest |");
        expectedOut.add("0032  616E6420 73686F75 6C642063 61757365  |and should cause|");
        expectedOut.add("0048  206D6F72 65207468 616E206F 6E65206C  | more than one l|");
        expectedOut.add("0064  696E6520 6F6E206F 75747075 742E      |ine on output.  |");
        expectedOut.add("");
        expectedOut.add("# 1 @ 0 for 21");
        expectedOut.add("0000  54686973 20697320 726F7720 6E756D62  |This is row numb|");
        expectedOut.add("0016  65722031 2E                          |er 1.           |");
        expectedOut.add("");
        Helper.compare(output, expectedOut);
        Assert.assertTrue(file1.delete());
        Assert.assertTrue(output.delete());
    }
}
