package com.obdobion.funnel.publisher;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class FixedLengthSysoutPublisher extends FixedLengthPublisher
{
    static final private Logger logger = LoggerFactory.getLogger(FixedLengthSysoutPublisher.class);

    public FixedLengthSysoutPublisher(final FunnelContext _context) throws ParseException, IOException
    {
        super(_context);
        logger.debug("fixed length sysout publisher activated");
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

    @Override
    public void reset () throws IOException
    {
        // intentionally empty
    }
}