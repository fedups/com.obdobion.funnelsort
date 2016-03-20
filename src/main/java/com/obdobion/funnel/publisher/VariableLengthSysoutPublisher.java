package com.obdobion.funnel.publisher;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;

import org.apache.log4j.Logger;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class VariableLengthSysoutPublisher extends VariableLengthPublisher
{
    static final private Logger logger = Logger.getLogger(VariableLengthSysoutPublisher.class);

    public VariableLengthSysoutPublisher(final FunnelContext _context) throws ParseException, IOException
    {
        super(_context);
    }

    @Override
    public void close () throws Exception
    {
        super.close();
        ((DataOutputStream) writer).close();
        logger.debug("closing SYSOUT");
    }

    @Override
    void openOutput (final FunnelContext _context) throws IOException, FileNotFoundException
    {
        writer = new DataOutputStream(System.out);
        logger.debug("writing SYSOUT");
    }
}