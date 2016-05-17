package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class CsvSysinReader extends VariableLengthSysinReader
{
    static final private Logger _logger = LoggerFactory.getLogger(CsvSysinReader.class);

    public CsvSysinReader(final FunnelContext _context) throws IOException, ParseException
    {
        super(_context);
        _logger.debug("csv sysin reader activated");
    }
}
