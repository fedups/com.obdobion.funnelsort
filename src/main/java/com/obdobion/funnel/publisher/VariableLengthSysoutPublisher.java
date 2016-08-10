package com.obdobion.funnel.publisher;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>VariableLengthSysoutPublisher class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class VariableLengthSysoutPublisher extends VariableLengthPublisher
{
    static final private Logger logger = LoggerFactory.getLogger(VariableLengthSysoutPublisher.class);

    /**
     * <p>Constructor for VariableLengthSysoutPublisher.</p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     * @throws java.text.ParseException if any.
     * @throws java.io.IOException if any.
     */
    public VariableLengthSysoutPublisher(final FunnelContext _context) throws ParseException, IOException
    {
        super(_context);
        logger.debug("variable length sysout publisher activated");
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception
    {
        super.close();
        ((DataOutputStream) writer).close();
        logger.debug("closing SYSOUT");
    }

    /**
     * @param _context
     */
    @Override
    void openOutput(final FunnelContext _context) throws IOException, FileNotFoundException
    {
        writer = new DataOutputStream(System.out);
        logger.debug("writing SYSOUT");
    }
}
