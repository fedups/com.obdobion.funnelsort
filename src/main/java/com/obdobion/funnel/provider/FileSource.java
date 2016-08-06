package com.obdobion.funnel.provider;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 *
 */
public class FileSource implements RandomAccessInputSource
{
    static final private Logger logger = LoggerFactory.getLogger(FileSource.class);

    final FunnelContext         context;
    RandomAccessFile[]          raf;

    public FileSource(final FunnelContext _context) throws ParseException, IOException
    {
        context = _context;
        raf = new RandomAccessFile[context.inputFileCount()];
    }

    @Override
    public void close() throws IOException, ParseException
    {
        for (int i = 0; i < context.inputFileCount(); i++)
        {
            raf[i].close();
            logger.debug("releasing original input source " + context.getInputFile(i).getAbsolutePath());
        }
    }

    @Override
    public void open() throws IOException, ParseException
    {
        for (int i = 0; i < context.inputFileCount(); i++)
        {
            raf[i] = new RandomAccessFile(context.getInputFile(i), "r");
            logger.debug("rereading original input source " + context.getInputFile(i).getAbsolutePath());
        }
    }

    @Override
    public int read(
            final int originalInputFileIndex,
            final byte[] originalBytes,
            final long originalLocation,
            final int originalSize)
                    throws IOException
    {
        raf[originalInputFileIndex].seek(originalLocation);
        int readSize = originalSize;
        if (originalBytes.length < originalSize)
            readSize = originalBytes.length;
        return raf[originalInputFileIndex].read(originalBytes, 0, readSize);
    }
}
