package com.obdobion.funnel;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.obdobion.Helper;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 * 
 */
public class BigTest
{

    @Test
    public void oneBigFile ()
        throws Throwable
    {
        Helper.initializeFor("TEST oneBigFile");

        final File output = new File("/tmp/oneBigFile");

        final List<String> in1 = new ArrayList<>();
        for (int x = 1; x <= 1000000; x++)
        {
            in1.add("" + x);
        }

        final List<String> expectedOut = new ArrayList<>();
        for (int x = 1000000; x >= 1; x--)
        {
            expectedOut.add("" + x);
        }

        final File file = Helper.createUnsortedFile("oneBigFile", in1);

        final FunnelContext context = Funnel.sort(file.getAbsolutePath()
                + " --col(int -o0 -l7 -n col1)"
                + " --orderby(col1 desc)"
                + " -o "
                + output.getAbsolutePath()
                + " --pow 8 --cacheWork --cacheInput"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", 1000000L, context.publisher.getWriteCount());
        Helper.compare(output, expectedOut);

        Assert.assertTrue(file.delete());
        Assert.assertTrue(output.delete());
    }
}
