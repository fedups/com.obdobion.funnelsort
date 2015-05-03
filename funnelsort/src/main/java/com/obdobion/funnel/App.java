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
    static public void abort (int code, Exception ex)
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
        LogManager.resetConfiguration();
        final String log4jParm = System.getProperty("log4j.configuration", workDir() + "/src/test/java/log4j.xml");
        DOMConfigurator.configure(log4jParm);

        try
        {
            Funnel.sort(args);

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