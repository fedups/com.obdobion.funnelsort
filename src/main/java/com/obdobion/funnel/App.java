package com.obdobion.funnel;

import java.text.ParseException;

import org.apache.log4j.LogManager;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * <p>App class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class App
{
    /**
     * <p>abort.</p>
     *
     * @param code a int.
     * @param ex a {@link java.lang.Exception} object.
     */
    static public void abort (final int code, final Exception ex)
    {
        ex.printStackTrace();
        System.exit(code);
    }

    /**
     * <p>main.</p>
     *
     * @param args a {@link java.lang.String} object.
     * @throws java.lang.Throwable if any.
     */
    public static void main (final String... args) throws Throwable
    {
        final AppContext cfg = new AppContext(workDir());

        LogManager.resetConfiguration();
        DOMConfigurator.configure(cfg.log4jConfigFileName);

        try
        {
            Funnel.sort(cfg, args);

        } catch (final ParseException e)
        {
            System.out.println(e.getMessage());
        }

        System.exit(0);
    }

    /**
     * @return
     */
    static private String workDir ()
    {
        return System.getProperty("user.dir");
    }
}
