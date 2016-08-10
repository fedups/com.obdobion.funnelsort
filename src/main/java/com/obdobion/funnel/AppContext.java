package com.obdobion.funnel;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;

import com.obdobion.argument.CmdLine;
import com.obdobion.argument.annotation.Arg;
import com.obdobion.argument.input.NamespaceParser;

/**
 * <p>AppContext class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class AppContext
{
    @Arg(caseSensitive = true, defaultValues = "UNKNOWN")
    public String   version;

    @Arg(multimin = 1)
    public String[] specPath;

    @Arg(longName = "log4j", caseSensitive = true)
    public String   log4jConfigFileName;

    /**
     * <p>Constructor for AppContext.</p>
     */
    public AppContext()
    {
        super();
    }

    /**
     * <p>Constructor for AppContext.</p>
     *
     * @param workingDirectory a {@link java.lang.String} object.
     * @throws java.io.IOException if any.
     * @throws java.text.ParseException if any.
     */
    public AppContext(final String workingDirectory) throws IOException, ParseException
    {
        final String configFileName = System.getProperty("funnel.config",
                workingDirectory + "/src/test/resources/funnel.cfg");
        new CmdLine().parse(NamespaceParser.getInstance(new File(configFileName)), this);
    }
}
