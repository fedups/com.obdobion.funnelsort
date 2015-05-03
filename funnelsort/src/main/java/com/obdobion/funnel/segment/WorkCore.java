package com.obdobion.funnel.segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.obdobion.funnel.Funnel;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class WorkCore implements WorkRepository
{
    static final private Logger logger               = Logger.getLogger(WorkCore.class);
    static final int            RecordHeaderSize     = 24;
    static final int            WriteBufferIncrement = 32768;

    final FunnelContext         context;
    final List<ByteBuffer>      writeBuffers;
    final List<Integer>         writeBufferLengths;
    long[]                      bufferStartingPosition;
    long                        writeFilePointer;
    ByteBuffer                  currentBuffer;

    /**
     * @param _context
     * @throws IOException
     */
    public WorkCore(final FunnelContext _context) throws IOException
    {
        this.context = _context;
        writeBuffers = new ArrayList<>();
        writeBufferLengths = new ArrayList<>();
        writeFilePointer = 0L;
        currentBuffer = ByteBuffer.wrap(new byte[WriteBufferIncrement], 0, WriteBufferIncrement);

        logger.debug("buffer size is " + WriteBufferIncrement + " bytes");
    }

    public void close () throws IOException
    {
        if (currentBuffer.position() > 0)
        {
            writeBuffers.add(currentBuffer);
            writeBufferLengths.add(currentBuffer.position());
        }

        if (writeFilePointer > 0)
            logger.debug(Funnel.ByteFormatter.format(writeFilePointer).trim() + " bytes used in work area");

        /*
         * Create the buffer starting position array. This will be used to
         * locate a buffer that contains a specific address.
         */
        bufferStartingPosition = new long[writeBufferLengths.size()];
        long pos = 0;
        for (int s = 0; s < writeBufferLengths.size(); s++)
        {
            bufferStartingPosition[s] = pos;
            pos += writeBufferLengths.get(s);
        }
    }

    public void delete () throws IOException
    {
        // intentionally empty
    }

    private long findBufferIndexForPosition (final long position)
    {
        int b = 0;
        int t = bufferStartingPosition.length;

        while (b < t)
        {
            final int m = (t + b) >>> 1;
            final long v = bufferStartingPosition[m];

            if (v == position)
                return m;

            if (position < v)
            {
                t = m;
                continue;
            }
            if (b == m)
                return b;
            b = m;
        }
        if (b == 0)
            return -1;
        return b;
    }

    private long formatRecord (final long position, final long begBufPos, final SourceProxyRecord rec)
    {
        currentBuffer.position((int) (position - begBufPos));

        rec.originalRecordNumber = currentBuffer.getLong();
        rec.originalLocation = currentBuffer.getLong();
        rec.originalSize = currentBuffer.getInt();
        rec.size = currentBuffer.getInt();
        rec.sortKey = new byte[rec.size];
        currentBuffer.get(rec.sortKey);

        return RecordHeaderSize + rec.size;
    }

    public void open () throws IOException
    {
        logger.trace("setting cache pointer to beginning");
        writeFilePointer = 0;
    }

    public long outputPosition ()
    {
        return writeFilePointer;
    }

    public long read (final long position, final SourceProxyRecord rec) throws IOException
    {
        final long begBufPos = setCurrentBuffer(position);
        return formatRecord(position, begBufPos, rec);
    }

    private long setCurrentBuffer (final long position)
    {
        final int s = (int) findBufferIndexForPosition(position);
        currentBuffer = writeBuffers.get(s);
        return bufferStartingPosition[s];
    }

    public long write (final SourceProxyRecord rec) throws IOException
    {
        final int sizeThisTime = RecordHeaderSize + rec.size;

        if (sizeThisTime + currentBuffer.position() >= currentBuffer.capacity())
        {
            writeBuffers.add(currentBuffer);
            writeBufferLengths.add(currentBuffer.position());
            currentBuffer = ByteBuffer.wrap(new byte[WriteBufferIncrement], 0, WriteBufferIncrement);
        }

        currentBuffer.putLong(rec.originalRecordNumber);
        currentBuffer.putLong(rec.originalLocation);
        currentBuffer.putInt(rec.originalSize);
        currentBuffer.putInt(rec.size);
        currentBuffer.put(rec.sortKey, 0, rec.size);

        final long startingPointer = writeFilePointer;
        writeFilePointer += sizeThisTime;

        return startingPointer;
    }
}