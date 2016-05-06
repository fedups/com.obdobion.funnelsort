package com.obdobion.funnel;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.obdobion.argument.CmdLine;
import com.obdobion.argument.input.IParserInput;
import com.obdobion.argument.input.NamespaceParser;

/**
 * @author Chris DeGreef
 *
 */
public class AppContext
{
    /*
     * Do not configure a logger in this class since it is used prior to setting
     * that up.
     */
    static private void defineVersion (final List<String> def)
    {
        def.add("-tString -k version --var version");
    }

    static private void defineSpecPath (final List<String> def)
    {
        def.add("-tString -k specPath --var specPath -m 1");
    }

    static private void defineLog4j (final List<String> def)
    {
        def.add("-tString -k log4j --var log4jConfigFileName -m 1");
    }

    final private CmdLine parser;
    String[]              args;

    public String         version;
    public String[]       specPath;
    public String         log4jConfigFileName;

    public AppContext()
    {
        super();
        parser = null;

        version = "UNKNOWN";
    }

    public AppContext(String workingDirectory) throws IOException, ParseException
    {
        final String configFileName = System.getProperty("funnel.config", workingDirectory
                + "/src/test/resources/funnel.cfg");

        parser = new CmdLine(null);

        final ArrayList<String> def = new ArrayList<>();

        defineVersion(def);
        defineSpecPath(def);
        defineLog4j(def);

        parser.compile(def);

        final IParserInput cmdline = NamespaceParser.getInstance(new File(configFileName));
        parser.parse(cmdline, this);

        postParseAnalysis();
    }

    private void postParseAnalysis ()
    {
        //
    }
}