package com.obdobion.funnel;

import org.junit.Test;

import com.obdobion.Helper;

/**
 * @author Chris DeGreef
 *
 */
public class AbbreviationTests
{

    @Test
    public void abbreviation1 ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        Funnel.sort(Helper.config(), " --so --ofn what --ci(s -o0 -l1 -n key)(i -o2 -l3 -n myNumber)"
            + " --ob(key asc)"
            + " --avg(-n avgNumber --ekwashun 'myNumber')"
            + " --fo(key)(-s1)(-eavgNumber -l4)");
    }
}
