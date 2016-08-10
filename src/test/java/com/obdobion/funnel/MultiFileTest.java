package com.obdobion.funnel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>MultiFileTest class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class MultiFileTest
{

    /**
     * <p>twoInputFilesMerged.</p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void twoInputFilesMerged ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        final File output = Helper.outFile(testName);

        final List<String> expectedOutput = new ArrayList<>();

        final List<String> in1 = new ArrayList<>();
        in1.add("line 1");
        in1.add("line 3");

        final List<String> in2 = new ArrayList<>();
        in2.add("line 2");
        in2.add("line 4");

        expectedOutput.add(in1.get(0));
        expectedOutput.add(in2.get(0));
        expectedOutput.add(in1.get(1));
        expectedOutput.add(in2.get(1));

        final File file = Helper.createUnsortedFile(testName, in1);
        final File file2 = Helper.createUnsortedFile(testName, in2);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getParent() + "/" + testName + "*"
            + " --nocacheinput"
            + " -o " + output.getAbsolutePath()
            + " --row 4 --variableIn CR LF --variableOut LF");

        Assert.assertEquals("records", 4L, context.getRecordCount());
        Assert.assertEquals("records", 4L, context.getWriteCount());
        Helper.compare(output, expectedOutput);

        Assert.assertTrue(file.delete());
        Assert.assertTrue(file2.delete());
        Assert.assertTrue(output.delete());
    }

    /**
     * <p>twoInputFilesWithReplace.</p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void twoInputFilesWithReplace ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);

        final List<String> out = new ArrayList<>();
        out.add("line 1");
        out.add("line 2");

        final File file = Helper.createUnsortedFile(testName, out);
        final File file2 = Helper.createUnsortedFile(testName, out);

        final FunnelContext context = Funnel.sort(Helper.config(), file.getAbsolutePath()
            + ","
            + file2.getAbsolutePath()
            + " --replace --row 2 -c original --variableIn CR LF --variableOut LF");

        Assert.assertEquals("records", 4L, context.getRecordCount());
        Helper.compare(file, out);
        Assert.assertTrue(file.delete());
        Assert.assertTrue(file2.delete());
    }
}
