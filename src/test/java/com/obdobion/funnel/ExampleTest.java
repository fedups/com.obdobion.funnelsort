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
public class ExampleTest
{

    @Test
    public void invalidCharError () throws Throwable
    {
        Helper.initializeFor("TEST invalidCharError");

        final List<String> in = new ArrayList<>();
        in.add("00,US,United States,\"48,208,387\",15.5,\"53,657\"");

        final File file = Helper.createUnsortedFile("csvParserCommentMarker", in);
        Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + "  --columnsIn "
                + " (-n FIPS -f 0 int) "
                + " (-n postalCode -f 1 string) "
                + " (-n stateName -f 2 string) "
                + " (-n povertyAll -f 3 int) "
                + " (-n povertyAllPercent -f 4 float) "
                + " (-n medianHouseholdIncome -f 5 int) "
                + " --csv(Default) "
                + "--orderBy(medianHouseholdIncome desc)"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());

    }

    @Test
    public void equationGreaterAndLess () throws Throwable
    {
        Helper.initializeFor("TEST invalidCharError");

        final List<String> out = new ArrayList<>();
        final List<String> in = new ArrayList<>();
        in.add("01AL905,682    19.2 42,917Alabama");
        in.add("02AK81,622     11.4 70,898Alaska");
        in.add("04AZ1,195,889  18.2 49,036Arizona");

        out.add(in.get(0));
        out.add(in.get(2));

        final File file = Helper.createUnsortedFile("csvParserCommentMarker", in);
        FunnelContext context = Funnel.sort(file.getAbsolutePath() + " -o" + file.getAbsolutePath()
                + " --columnsIn"
                + " (-n FIPS --length 2 int)"
                + " (-n postalCode --length 2 string)"
                + " (-n povertyAll --length 11 int)"
                + " (-n povertyAllPercent --length 4 float)"
                + " (-n medianHouseholdIncome --length 7 int)"
                + " (-n stateName --length 21 string)"
                + "--where 'medianHouseholdIncome >= 40000 && medianHouseholdIncome < 50000'"
                + Helper.DEFAULT_OPTIONS);

        Assert.assertEquals("records", out.size(), context.publisher.getWriteCount());
        Helper.compare(file, out);
        Assert.assertTrue("delete " + file.getAbsolutePath(), file.delete());

    }

}
