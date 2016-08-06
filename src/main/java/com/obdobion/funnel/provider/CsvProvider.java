package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.AppContext;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class CsvProvider extends VariableLengthProvider
{
    static final Logger _logger = LoggerFactory.getLogger(CsvProvider.class);

    boolean             includeColumn[];

    /**
     * for testing only
     *
     * @param _includeColumn
     * @throws IOException
     */
    public CsvProvider(final boolean _includeColumn[]) throws Exception
    {
        this(new FunnelContext(new AppContext()));
        includeColumn = _includeColumn;
    }

    public CsvProvider(final FunnelContext _context) throws IOException, ParseException
    {
        super(_context);
        logger.debug("CSV file provider activated");
        if (_context == null || _context.getInputColumnDefs() == null)
            return;
        /*
         * Find out which fields we really care about. No sense in moving around
         * bytes or analyzing fields we ultimately won't care about.
         */
        int highestKeyedColumnNumber = -1;

        for (final KeyPart kdef : _context.getInputColumnDefs())
            if (kdef.csvFieldNumber > highestKeyedColumnNumber)
                highestKeyedColumnNumber = kdef.csvFieldNumber;
        includeColumn = new boolean[highestKeyedColumnNumber + 1];
        for (final KeyPart kdef : _context.getInputColumnDefs())
            includeColumn[kdef.csvFieldNumber] = true;
    }

    @Override
    public long actualNumberOfRows()
    {
        return super.actualNumberOfRows()
                - (context.getCsv().header
                        ? 1
                        : 0);
    }

    @Override
    void assignReaderInstance() throws IOException, ParseException
    {
        if (context.isSysin())
            reader = new CsvSysinReader(context);
        else if (context.isCacheInput())
            reader = new CsvCacheReader(context);
        else
            reader = new CsvFileReader(context);
    }

    public byte[][] decodeCsv(final byte[] input, final int inputLength, final CSVFormat csvFormat)
            throws IOException
    {
        final byte[][] field = new byte[includeColumn.length][];

        try (final CSVParser csvparser = CSVParser.parse(new String(input, 0, inputLength), csvFormat))
        {
            final CSVRecord csvrecord = csvparser.getRecords().get(0);
            final Iterator<String> fields = csvrecord.iterator();
            for (int fNum = 0; fields.hasNext(); fNum++)
            {
                final String fieldAsString = fields.next();

                if (fNum >= includeColumn.length)
                    return field;

                if (fNum < includeColumn.length)
                    if (includeColumn[fNum])
                        field[fNum] = fieldAsString.getBytes();
            }
            return field;
        }
    }

    /**
     * A special case of not selecting the row is when a header is expected on
     * the csv file. This is one line and is to be kept for writing but ignored
     * for sorting. The publisher needs to be sensitive to the csv header
     * possibility and force the header out at the front of the file.
     *
     */
    @Override
    boolean isRowSelected(final int byteCount)
    {
        if (context.getCsv().header && context.getCsv().headerContents == null)
        {
            context.getCsv().headerContents = new byte[byteCount];
            System.arraycopy(row, 0, context.getCsv().headerContents, 0, byteCount);
            return false;
        }
        return true;
    }

    @Override
    KeyContext postReadKeyProcessing(final int byteCount) throws IOException
    {
        KeyContext kContext = null;
        try
        {
            final byte[][] data = decodeCsv(row, byteCount, context.getCsv().format);
            kContext = context.keyHelper.extractKey(data, getContinuousRecordNumber());

        } catch (final Exception e)
        {
            throw new IOException(e);
        }
        return kContext;
    }

    @Override
    void preSelectionExtract(final int byteCount) throws Exception
    {
        final byte[][] data = decodeCsv(row, byteCount, context.getCsv().format);
        context.columnHelper.extract(context, data, getContinuousRecordNumber(), byteCount);
    }

    byte[] unquote(final byte[] input, final int _start, final int _end, final byte quoteByte)
    {
        int start = _start;
        int end = _end;

        for (; start <= end; start++)
        {
            if (input[start] == (byte) ' ' || input[start] == (byte) '\t' || input[start] == quoteByte)
                continue;
            break;
        }
        for (; start <= end; end--)
        {
            if (input[end] == 32 || input[end] == 8 || input[end] == quoteByte)
                continue;
            break;
        }
        final int len = (end - start) + 1;
        final byte[] bb = new byte[len];
        System.arraycopy(input, start, bb, 0, len);
        return bb;
    }
}