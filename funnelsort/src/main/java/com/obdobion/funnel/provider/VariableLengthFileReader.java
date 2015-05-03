package com.obdobion.funnel.provider;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.log4j.Logger;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class VariableLengthFileReader implements InputReader
{
    static final Logger logger                = Logger.getLogger(VariableLengthFileReader.class);

    final FunnelContext context;
    File                inFile;
    RandomAccessFile    raf;
    long                startPosition;
    final byte          bb[];
    int                 bbInUse;
    final byte[]        separator;
    static int          defaultCharBufferSize = 32768;
    int                 bbNextPointer;
    boolean             eof;

    public VariableLengthFileReader(
            final FunnelContext _context)
            throws IOException
    {
        this(
            _context, defaultCharBufferSize);
    }

    public VariableLengthFileReader(
            final FunnelContext _context, final int sz)
            throws IOException
    {
        assert sz > 0 : "Buffer size <= 0";
        this.context = _context;
        bb = new byte[sz];
        this.separator = context.endOfRecordDelimiter;

        open(_context.getInputFile(context.inputFileIndex()));
    }

    public void close ()
        throws IOException
    {
        raf.close();
        logger.debug("loaded " + context.getInputFile(context.inputFileIndex()).getAbsolutePath());
    }

    private int fillBB ()
        throws IOException
    {
        startPosition = raf.getFilePointer();
        bbInUse = raf.read(bb);
        bbNextPointer = 0;
        if (bbInUse == -1)
            eof = true;
        return bbInUse;
    }

    public long length ()
        throws IOException
    {
        return raf.length();
    }

    public void open (final File inputFile)
        throws IOException
    {
        bbNextPointer = 0;
        this.inFile = context.getInputFile(context.inputFileIndex());
        this.raf = new RandomAccessFile(inFile, "r");
        eof = false;
        fillBB();
    }

    public long position ()
        throws IOException
    {
        return startPosition + bbNextPointer;
    }

    public int read (final byte[] row)
        throws IOException
    {
        if (eof)
            return -1;

        int rowNextPointer = 0;
        int sepNextPointer = 0;
        for (rowNextPointer = 0;; bbNextPointer++)
        {
            /*
             * First see of the buffer is empty - refill it.
             */
            if (bbNextPointer >= bbInUse)
            {
                if (fillBB() <= 0)
                {
                    /*
                     * end of file without end of record, this is ok.
                     */
                    if (rowNextPointer == 0)
                        return -1;
                    /*
                     * We have hit the end of the file and did not find an end
                     * of line for the last bytes we did find. Return the row
                     * for what is there.
                     */
                    logger.warn("assuming a line terminator at end of file where " + rowNextPointer
                        + " unterminated bytes were found");
                    return rowNextPointer;
                }
            }
            /*
             * Then see of this character is the next expected character in the
             * end of row sequence.
             */
            if (bb[bbNextPointer] == separator[sepNextPointer])
            {
                if (sepNextPointer == separator.length - 1)
                {
                    bbNextPointer++;
                    return rowNextPointer;
                }
                sepNextPointer++;
                continue;
            }
            /*
             * Something that started to look like the end of row turned out not
             * to be. So give it to the caller.
             */
            if (sepNextPointer > 0)
            {
                for (int sp = 0; sp < sepNextPointer; sp++)
                    row[rowNextPointer++] = separator[sp];
                sepNextPointer = 0;
            }
            /*
             * transfer the byte to the row.
             */
            row[rowNextPointer++] = bb[bbNextPointer];
        }
    }
}
