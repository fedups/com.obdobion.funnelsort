package com.obdobion.funnel;

import java.text.ParseException;

import org.apache.log4j.LogManager;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * @author Chris DeGreef
 *
 */
public class App
{
    /**
     * @param code
     * @param ex
     */
    static public void abort (final int code, final Exception ex)
    {
        ex.printStackTrace();
        System.exit(code);
    }

    /**
     * @param args
     * @throws Throwable
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