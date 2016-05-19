package com.obdobion.funnel.provider;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.publisher.RandomAccessInputSource;

/**
 * @author Chris DeGreef
 *
 */
abstract public class AbstractInputCache implements RandomAccessInputSource
{
    static final private Logger logger     = LoggerFactory.getLogger(AbstractInputCache.class);

    static final int            BufferSize = 1 << 15;

    /*
     * public for junit only
     */
    static public int findBufferIndexForPosition (
        final long position,
        final long[] startingPositions)
    {
        int b = 0;
        int t = startingPositions.length;

        while (b < t)
        {

            final int m = (t + b) >>> 1;
            final long v = startingPositions[m];

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

    final FunnelContext    context;
    final InputStream      source;
    final List<ByteBuffer> sourceBuffers;
    /*
     * keep this value separate for efficiency
     */
    final int              sourceBuffersSize;

    final long[]           bufferStartingPosition;
    long                   currentFilePosition;
    int                    currentBufferIndex;
    ByteBuffer             currentBuffer;

    long                   length;

    public AbstractInputCache(
            final FunnelContext _context, final InputStream _source)
            throws IOException
    {
        logger.debug("read buffer size is " + BufferSize + " bytes");

        this.context = _context;
        this.source = _source;
        sourceBuffers = new ArrayList<>();

        loadUntilSourceIsDepleted();
        sourceBuffersSize = sourceBuffers.size();
        bufferStartingPosition = new long[sourceBuffersSize];
        computeStartingPositionsOfTheBuffers();

        logger.debug(sourceBuffersSize + " buffers loaded into memory");
        logger.debug(length() + " bytes total in all buffers");

        postOpenVerification();

        currentFilePosition = 0L;
        currentBufferIndex = 0;
    }

    public void close ()
        throws IOException
    {
        // Intentionally empty
    }

    void computeStartingPositionsOfTheBuffers ()
    {
        long runningTotal = 0;
        for (int b = 0; b < sourceBuffersSize; b++)
        {
            bufferStartingPosition[b] = runningTotal;
            runningTotal += sourceBuffers.get(b).capacity();
        }
    }

    public boolean eof ()
    {
        if (currentBufferIndex + 1 < sourceBuffersSize)
            return false;
        if (currentBufferIndex >= sourceBuffersSize)
            return true;
        if (currentBuffer == null)
            return false;
        return !currentBuffer.hasRemaining();
    }

    public long length ()
    {
        return length;
    }

    void loadUntilSourceIsDepleted ()
        throws IOException
    {
        /*
         * Checking available bytes here may inhibit command line typing of
         * unsorted data. This is really only done in order to support test
         * cases. It used to be "true", forever.
         */
        try
        {
            if (source.available() <= 0)
            {
                logger.debug("input source is not available");
                return;
            }
        } catch (final IOException e)
        {
            logger.debug("input source is not available, " + e.getMessage());
            return;
        }
        while (true)
        {
            final byte[] readBuf = new byte[BufferSize];
            final int bytesRead = source.read(readBuf);
            if (bytesRead == -1)
                break;
            length += bytesRead;
            if (bytesRead < BufferSize)
            {
                final ByteBuffer bb = ByteBuffer.wrap(Arrays.copyOf(readBuf, bytesRead));
                sourceBuffers.add(bb);
                break;
            }
            final ByteBuffer bb = ByteBuffer.wrap(readBuf);
            sourceBuffers.add(bb);
        }
    }

    public void open ()
        throws IOException
    {
        // Intentionally empty
    }

    public long position ()
    {
        return currentFilePosition;
    }

    abstract void postOpenVerification () throws IOException;

    /**
     * This method should not be called if there are no bytes available. Use
     * !eof() first.
     *
     * @return
     */
    public int read (
        final int inputFileIndex,
        final byte[] bytes,
        final long position,
        final int _length)
    {
        currentBufferIndex = findBufferIndexForPosition(position, bufferStartingPosition);

        currentBuffer = sourceBuffers.get(currentBufferIndex);
        final long begBufPos = bufferStartingPosition[currentBufferIndex];

        final int bbPosition = (int) (position - begBufPos);
        final int bbCap = currentBuffer.capacity();
        if ((bbPosition + _length) > bbCap)
        {
            final ByteBuffer concat = ByteBuffer.wrap(bytes);
            int bytesToCopy = _length;
            if (bbPosition < bbCap)
            {
                final int bytesAtEndOfBuffer = bbCap - bbPosition;
                /*
                 * An array() request does not honor the position of the buffer.
                 * So do not start at 0 and don't bother playing around with the
                 * position since it is no longer needed; the buffer will have
                 * been consumed.
                 */
                concat.put(currentBuffer.array(), bbPosition, bytesAtEndOfBuffer);
                currentBuffer.position(bbCap - 1);
                bytesToCopy -= bytesAtEndOfBuffer;
            }
            if (bytesToCopy > 0)
            {
                currentBufferIndex++;
                currentBuffer = sourceBuffers.get(currentBufferIndex);
                currentBuffer.position(0);
                concat.put(currentBuffer.array(), 0, bytesToCopy);
            }
        } else
        {
            currentBuffer.position(bbPosition);
            currentBuffer.get(bytes, 0, _length);
        }

        currentFilePosition = bbPosition + _length;
        return _length;
    }

    /**
     * This method should not be called if there are no bytes available. Use
     * !eof() first.
     *
     * @return
     */
    public byte readNextByte ()
    {
        currentBuffer = sourceBuffers.get(currentBufferIndex);

        if (!currentBuffer.hasRemaining())
        {
            currentBufferIndex++;
            currentBuffer = sourceBuffers.get(currentBufferIndex);
        }
        currentFilePosition += 1;
        return currentBuffer.get();
    }
}