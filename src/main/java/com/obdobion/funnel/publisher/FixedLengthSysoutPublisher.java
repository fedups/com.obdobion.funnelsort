package com.obdobion.funnel.publisher;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 * 
 */
public class FixedLengthSysoutPublisher extends FixedLengthPublisher
{
    static final private Logger logger = Logger.getLogger(FixedLengthSysoutPublisher.class);

    public FixedLengthSysoutPublisher(final FunnelContext _context)
    {
        super(_context);
    }

    @Override
    public void close () throws IOException
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

    @Override
    public void reset () throws IOException
    {
        // intentionally empty
    }
}