package com.obdobion.funnel.publisher;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.Formatter;

import com.obdobion.funnel.orderby.KeyPart;
import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.parameters.HexDump;
import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 *
 */
abstract public class VariableLengthPublisher extends AbstractPublisher
{
    private static final byte[] HEX_CHARS = new byte[16];

    static
    {
        byte b;
        int i = 0;
        for (b = '0'; b <= '9'; b++)
        {
            HEX_CHARS[i++] = b;
        }
        for (b = 'A'; b <= 'F'; b++)
        {
            HEX_CHARS[i++] = b;
        }
    }

    private static int appendDecChars (final int i, final byte[] chars, final int startOffset)
    {
        int offset;
        int value;

        offset = startOffset;
        value = i;

        StringBuilder sb;
        final Formatter fmt = new Formatter(sb = new StringBuilder());
        fmt.format("%04d", value);
        fmt.close();

        final byte[] ba = sb.toString().getBytes();

        int bao = 0;
        // chars[offset++] = ba[bao++];
        // chars[offset++] = ba[bao++];
        // chars[offset++] = ba[bao++];
        // chars[offset++] = ba[bao++];
        chars[offset++] = ba[bao++];
        chars[offset++] = ba[bao++];
        chars[offset++] = ba[bao++];
        chars[offset++] = ba[bao++];

        return offset;
    }

    private static int appendHexChars (final byte b, final byte[] chars, final int startOffset)
    {
        int byteAsInt, offset;

        byteAsInt = b;

        offset = startOffset;
        chars[offset++] = HEX_CHARS[byteAsInt >> 4 & 0x0F];
        chars[offset++] = HEX_CHARS[byteAsInt & 0x0F];
        return offset;

    }

    private static final int dumpBuffSize (final int p_charsPerRow)
    {
        return (13 + (p_charsPerRow * 2) + (p_charsPerRow / 4) + p_charsPerRow);
    }

    public VariableLengthPublisher(final FunnelContext _context) throws ParseException, IOException
    {
        super(_context);
        this.originalBytes = new byte[1024];
    }

    /**
     * @param item
     * @param rawData
     * @throws IOException
     */
    void formatHexDumpAndWrite (final SourceProxyRecord item, final byte[] rawData)
        throws IOException, Exception
    {
        StringWriter sw;
        PrintWriter pw;
        String line;

        pw = new PrintWriter(sw = new StringWriter());
        pw.printf("# %d @ %d for %d",
            item.getOriginalRecordNumber(), item.originalLocation, item.originalSize);
        line = sw.toString();
        write(line.getBytes(), 0, line.length());
        newLine();

        for (final HexDump dumpee : context.hexDumps)
        {
            final KeyPart column = context.columnHelper.get(dumpee.columnName);
            if (column != null)
            {
                /*
                 * field header
                 */
                pw = new PrintWriter(sw = new StringWriter());
                pw.printf("%s", dumpee.columnName);

                line = sw.toString();
                write(line.getBytes(), 0, line.length());
                newLine();
            }

            if (column != null)
                hexDump(column.getContentsAsByteArray(), column.offset);
            else
                hexDump(rawData, 0, item.originalSize);
        }
        /*
         * record separator
         */
        newLine();
    }

    @Override
    void formatOutputAndWrite (final SourceProxyRecord item, final byte[] rawData)
        throws IOException, Exception
    {
        if (context.hexDumps == null || context.formatOutDefs != null)
        {
            /*
             * The --format can be empty, causing the entire row to be written,
             * unless a --hexdump is requested. In that case we would not write
             * the row.
             */
            context.formatOutHelper.format(this, rawData, item.originalSize, item, true);
            newLine();
        }

        super.formatOutputAndWrite(item, rawData);

        if (context.hexDumps != null)
            formatHexDumpAndWrite(item, rawData);
    }

    private void hexDump (final byte[] bytesToDump, final int fieldOffsetInRow) throws IOException
    {
        hexDump(bytesToDump, fieldOffsetInRow, bytesToDump.length);
    }

    private void hexDump (final byte[] bytesToDump, final int fieldOffsetInRow, final int length) throws IOException
    {
        hexDump(bytesToDump, fieldOffsetInRow, length,
            16,
            new byte[dumpBuffSize(16)]);
    }

    private void hexDump (final byte[] p_array,
        final int p_startPrintedOffset,
        final int p_numBytes,
        final int p_charsPerRow,
        final byte[] byteBuffer) throws IOException
    {
        byte asciiByteValue;
        int offset, i, relOffset, endOffset, outLen, printedOffset;
        /*
         * Compute endOffset and adjust if it would cause us to index out of
         * bounds. endOffset is 1 more than the last p_array cell we will
         * reference.
         */

        endOffset = p_numBytes;
        if (endOffset > p_array.length)
            endOffset = p_array.length;

        printedOffset = p_startPrintedOffset;
        offset = 0;

        while (offset < endOffset)
        {
            // Append the offset as a hex number and 1 of the 2 spaces that
            // follow it.

            outLen = 0;
            outLen = appendDecChars(printedOffset, byteBuffer, outLen);
            byteBuffer[outLen++] = ' ';

            // Now build the hex representation.
            // On the 0th, 4th, 8th, , etc. element we'll pre-pend a blank
            // If the relOffset is >= endOffset, we put in 2 blanks, not hex
            // digits.

            for (i = 0; i < p_charsPerRow; i++)
            {
                relOffset = offset + i;
                if ((i & 0x03) == 0)
                    byteBuffer[outLen++] = ' ';
                if (relOffset < endOffset)
                    outLen = appendHexChars(p_array[relOffset], byteBuffer, outLen);
                else
                {
                    byteBuffer[outLen++] = ' ';
                    byteBuffer[outLen++] = ' ';
                }
            }

            /*
             * Now append 2 spaces and the "| delimiter; we'll format the ASCII
             * portion now.
             */
            byteBuffer[outLen++] = ' ';
            byteBuffer[outLen++] = ' ';
            byteBuffer[outLen++] = '|';

            for (i = 0; i < p_charsPerRow; i++)
            {
                relOffset = offset + i;
                if (relOffset < endOffset)
                {
                    asciiByteValue = p_array[relOffset];
                    if (asciiByteValue < ' ' || asciiByteValue > '~')
                        asciiByteValue = '.';
                } else
                {
                    asciiByteValue = ' ';
                }
                byteBuffer[outLen++] = asciiByteValue;
            }
            /*
             * Add on final "|" at the end, print to print stream, and increment
             * offset by charsPerRow.
             */
            byteBuffer[outLen++] = '|';

            write(byteBuffer, 0, outLen);
            newLine();

            offset += p_charsPerRow;
            printedOffset += p_charsPerRow;
        }
    }

    @Override
    void loadOriginalBytes (final int originalFileNumber, final SourceProxyRecord item)
        throws IOException
    {
        if (item.originalSize > originalBytes.length)
        {
            originalBytes = new byte[item.originalSize + 1024];
        }
        /*
         * Make sure to delimit the current record length in the input buffer.
         */
        originalBytes[item.originalSize] = 0x00;
        super.loadOriginalBytes(originalFileNumber, item);
    }

    @Override
    void newLine () throws IOException
    {
        write(context.endOfRecordDelimiterOut, 0, context.endOfRecordDelimiterOut.length);
    }

    @Override
    void publishHeader () throws IOException
    {
        super.publishHeader();
        /*
         * This is the first time publishing to this file. So lets see if there
         * is a header tucked away in the csv context area. We will write that
         * out first.
         */
        if (context.csv != null && context.csv.header && context.csv.headerContents != null)
        {
            write(context.csv.headerContents, 0, context.csv.headerContents.length);
            newLine();
        }
    }
}