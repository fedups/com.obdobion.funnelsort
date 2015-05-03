package com.obdobion.funnel.publisher;

import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.log4j.Logger;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 * 
 */
public class FileSource implements RandomAccessInputSource
{
    static final private Logger logger = Logger.getLogger(FileSource.class);

    final FunnelContext         context;
    RandomAccessFile[]          raf;

    public FileSource(
            final FunnelContext _context)
    {
        this.context = _context;
        raf = new RandomAccessFile[context.inputFileCount()];
    }

    public void close ()
        throws IOException
    {
        for (int i = 0; i < context.inputFileCount(); i++)
        {
            raf[i].close();
            logger.debug("releasing original input source " + context.getInputFile(i).getAbsolutePath());
        }
    }

    public void open ()
        throws IOException
    {
        for (int i = 0; i < context.inputFileCount(); i++)
        {
            raf[i] = new RandomAccessFile(
                context.getInputFile(i), "r");
            logger.debug("rereading original input source " + context.getInputFile(i).getAbsolutePath());
        }
    }

    public int read (
        final int originalInputFileIndex,
        final byte[] originalBytes,
        final long originalLocation,
        final int originalSize)
        throws IOException
    {
        raf[originalInputFileIndex].seek(originalLocation);
        return raf[originalInputFileIndex].read(originalBytes, 0, originalSize);
    }
}
