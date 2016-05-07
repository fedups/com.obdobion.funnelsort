package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import com.obdobion.funnel.AppContext;
import com.obdobion.funnel.orderby.KeyContext;
import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class VariableLengthCsvProvider extends VariableLengthProvider
{

    static final Logger _logger = Logger.getLogger(VariableLengthCsvProvider.class);

    boolean             includeColumn[];

    /**
     * for testing only
     *
     * @param context
     * @throws IOException
     */
    public VariableLengthCsvProvider(final boolean _includeColumn[]) throws Exception
    {
        this(new FunnelContext(new AppContext()));
        this.includeColumn = _includeColumn;
    }

    public VariableLengthCsvProvider(final FunnelContext _context) throws IOException, ParseException
    {
        super(_context);
        if (_context == null || _context.inputColumnDefs == null)
            return;
        /*
         * Find out which fields we really care about. No sense in moving around
         * bytes or analyzing fields we ultimately won't care about.
         */
        int highestKeyedColumnNumber = -1;

        for (final KeyPart kdef : _context.inputColumnDefs)
        {
            if (kdef.csvFieldNumber > highestKeyedColumnNumber)
                highestKeyedColumnNumber = kdef.csvFieldNumber;
        }
        includeColumn = new boolean[highestKeyedColumnNumber + 1];
        for (final KeyPart kdef : _context.inputColumnDefs)
            includeColumn[kdef.csvFieldNumber] = true;
    }

    @Override
    public long actualNumberOfRows ()
    {
        return super.actualNumberOfRows()
            - (context.csv.header
                    ? 1
                    : 0);
    }

    public byte[][] decodeCsv (final byte[] input, final int inputLength, CSVFormat csvFormat)
        throws IOException
    {
        final byte[][] field = new byte[includeColumn.length][];

        CSVParser csvparser = CSVParser.parse(new String(input, 0, inputLength), csvFormat);
        try
        {
            CSVRecord csvrecord = csvparser.getRecords().get(0);
            Iterator<String> fields = csvrecord.iterator();
            for (int fNum = 0; fields.hasNext(); fNum++)
            {
                String fieldAsString = fields.next();

                if (fNum >= includeColumn.length)
                    return field;

                if (fNum < includeColumn.length)
                    if (includeColumn[fNum])
                        field[fNum] = fieldAsString.getBytes();
            }

            return field;
        } finally
        {
            csvparser.close();
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
    boolean isRowSelected (final int byteCount)
    {
        if (context.csv.header && context.csv.headerContents == null)
        {
            context.csv.headerContents = new byte[byteCount];
            System.arraycopy(row, 0, context.csv.headerContents, 0, byteCount);
            return false;
        }
        return true;
    }

    @Override
    KeyContext postReadKeyProcessing (final int byteCount) throws IOException
    {
        KeyContext kContext = null;
        try
        {
            final byte[][] data = decodeCsv(row, byteCount, context.csv.format);
            kContext = context.keyHelper.extractKey(data, recordNumber);

        } catch (final Exception e)
        {
            throw new IOException(e);
        }
        return kContext;
    }

    @Override
    void preSelectionExtract (int byteCount) throws Exception
    {
        final byte[][] data = decodeCsv(row, byteCount, context.csv.format);
        context.columnHelper.extract(context, data, recordNumber, byteCount);
    }

    byte[] unquote (final byte[] input, final int _start, final int _end, final byte quoteByte)
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