package com.obdobion.funnel;

import org.junit.Test;

import com.obdobion.Helper;

/**
 * <p>AbbreviationTests class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 * @since 1.6.6
 */
public class AbbreviationTests
{

    /**
     * <p>abbreviation1.</p>
     *
     * @throws java.lang.Throwable if any.
     */
    @Test
    public void abbreviation1 ()
        throws Throwable
    {
        final String testName = Helper.testName();
        Helper.initializeFor(testName);
        Funnel.sort(Helper.config(), " --so --ofn what --ci(s -o0 -l1 -n key)(i -o2 -l3 -n myNumber)"
            + " --ob(key Asc)"
            + " --avg(-n avgNumber --equation 'myNumber')"
            + " --fo(key)(-s1)(-eavgNumber -l4)");
    }
}
