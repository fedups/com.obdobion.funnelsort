package com.obdobion.funnel.segment;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.Funnel;
import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class WorkFile implements WorkRepository
{
    static final private Logger logger           = LoggerFactory.getLogger(WorkFile.class);
    static final int            RecordHeaderSize = 28;
    static final int            WriteBufferSize  = 32768;

    final FunnelContext         context;
    final File                  file;
    private RandomAccessFile    raf;
    final byte[]                writeBuffer;
    final ByteBuffer            bb;
    long                        writeFilePointer;

    /**
     * @param _context
     * @throws IOException
     */
    public WorkFile(final FunnelContext _context) throws IOException
    {
        this.context = _context;
        file = File.createTempFile("funnel.", ".tmp", _context.workDirectory);
        file.deleteOnExit();
        writeBuffer = new byte[WriteBufferSize];
        bb = ByteBuffer.wrap(writeBuffer, 0, WriteBufferSize);

        logger.debug("buffer size is " + WriteBufferSize + " bytes");
    }

    public void close () throws IOException
    {
        if (bb.position() != 0)
            flushWritesToDisk();
        raf.close();
        /*
         * Only show the statistic when the file has been written to.
         */
        if (writeFilePointer > 0)
            logger.debug(Funnel.ByteFormatter.format(writeFilePointer).trim() + " bytes in work file");
        logger.debug("closed " + file.getAbsolutePath());
    }

    public void delete () throws IOException
    {
        if (file.delete())
            logger.debug("deleted " + file.getAbsolutePath());
        else
            logger.debug("not deleted, " + file.getAbsolutePath() + " not found");
    }

    void flushWritesToDisk () throws IOException
    {
        raf.write(bb.array(), 0, bb.position());
        bb.position(0);
    }

    public FunnelContext getContext ()
    {
        return context;
    }

    public void open () throws IOException
    {
        raf = new RandomAccessFile(file, "rw");
        bb.position(0);
        writeFilePointer = 0L;
        logger.debug("opened " + file.getAbsolutePath());
    }

    public long outputPosition ()
    {
        return writeFilePointer;
    }

    public long read (final long position, final SourceProxyRecord rec) throws IOException
    {
        raf.seek(position);

        rec.originalInputFileIndex = raf.readInt();
        rec.originalRecordNumber = raf.readLong();
        rec.originalLocation = raf.readLong();
        rec.originalSize = raf.readInt();
        rec.size = raf.readInt();
        rec.sortKey = new byte[rec.size];
        final int readSize = raf.read(rec.sortKey);

        return RecordHeaderSize + readSize;
    }

    public long write (final SourceProxyRecord rec) throws IOException
    {
        final int sizeThisTime = RecordHeaderSize + rec.size;

        if (sizeThisTime + bb.position() >= WriteBufferSize)
            flushWritesToDisk();

        bb.putInt(rec.originalInputFileIndex);
        bb.putLong(rec.originalRecordNumber);
        bb.putLong(rec.originalLocation);
        bb.putInt(rec.originalSize);
        bb.putInt(rec.size);
        bb.put(rec.sortKey, 0, rec.size);

        final long startingPointer = writeFilePointer;
        writeFilePointer += sizeThisTime;

        return startingPointer;
    }
}