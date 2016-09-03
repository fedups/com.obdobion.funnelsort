package com.obdobion.funnel.segment;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.Funnel;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * <p>
 * WorkCore class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class WorkCore implements WorkRepository
{
    static final private Logger logger               = LoggerFactory.getLogger(WorkCore.class);
    static final int            RecordHeaderSize     = 28;
    static final int            WriteBufferIncrement = 32768;

    final FunnelContext         context;
    final List<ByteBuffer>      writeBuffers;
    final List<Integer>         writeBufferLengths;
    long[]                      bufferStartingPosition;
    long                        writeFilePointer;
    ByteBuffer                  currentBuffer;

    /**
     * <p>
     * Constructor for WorkCore.
     * </p>
     *
     * @param _context a {@link com.obdobion.funnel.parameters.FunnelContext}
     *            object.
     * @throws java.io.IOException if any.
     */
    public WorkCore(final FunnelContext _context) throws IOException
    {
        context = _context;
        writeBuffers = new ArrayList<>();
        writeBufferLengths = new ArrayList<>();
        writeFilePointer = 0L;
        currentBuffer = ByteBuffer.wrap(new byte[WriteBufferIncrement], 0, WriteBufferIncrement);

        logger.debug("buffer size is " + WriteBufferIncrement + " bytes");
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException
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

    /** {@inheritDoc} */
    @Override
    public void delete() throws IOException
    {
        // intentionally empty
    }

    private long findBufferIndexForPosition(final long position)
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

    private long formatRecord(final long position, final long begBufPos, final SourceProxyRecord rec)
    {
        currentBuffer.position((int) (position - begBufPos));

        rec.originalInputFileIndex = currentBuffer.getInt();
        rec.setOriginalRecordNumber(currentBuffer.getLong());
        rec.originalLocation = currentBuffer.getLong();
        rec.originalSize = currentBuffer.getInt();
        rec.size = currentBuffer.getInt();
        rec.sortKey = new byte[rec.size];
        currentBuffer.get(rec.sortKey);

        return RecordHeaderSize + rec.size;
    }

    /** {@inheritDoc} */
    @Override
    public FunnelContext getContext()
    {
        return context;
    }

    /** {@inheritDoc} */
    @Override
    public void open() throws IOException
    {
        logger.trace("setting cache pointer to beginning");
        writeFilePointer = 0;
    }

    /** {@inheritDoc} */
    @Override
    public long outputPosition()
    {
        return writeFilePointer;
    }

    /** {@inheritDoc} */
    @Override
    public long read(final long position, final SourceProxyRecord rec) throws IOException
    {
        final long begBufPos = setCurrentBuffer(position);
        return formatRecord(position, begBufPos, rec);
    }

    private long setCurrentBuffer(final long position)
    {
        final int s = (int) findBufferIndexForPosition(position);
        currentBuffer = writeBuffers.get(s);
        return bufferStartingPosition[s];
    }

    /** {@inheritDoc} */
    @Override
    public long write(final SourceProxyRecord rec) throws IOException
    {
        final int sizeThisTime = RecordHeaderSize + rec.size;

        if (sizeThisTime + currentBuffer.position() >= currentBuffer.capacity())
        {
            writeBuffers.add(currentBuffer);
            writeBufferLengths.add(currentBuffer.position());
            currentBuffer = ByteBuffer.wrap(new byte[WriteBufferIncrement], 0, WriteBufferIncrement);
        }

        currentBuffer.putInt(rec.originalInputFileIndex);
        currentBuffer.putLong(rec.getOriginalRecordNumber());
        currentBuffer.putLong(rec.originalLocation);
        currentBuffer.putInt(rec.originalSize);
        currentBuffer.putInt(rec.size);
        currentBuffer.put(rec.sortKey, 0, rec.size);

        final long startingPointer = writeFilePointer;
        writeFilePointer += sizeThisTime;

        return startingPointer;
    }
}
