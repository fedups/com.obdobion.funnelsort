package com.obdobion.funnel;

import java.io.IOException;

import com.obdobion.funnel.segment.SourceProxyRecord;

/**
 * @author Chris DeGreef
 * 
 */
public interface FunnelDataPublisher
{
    public void close () throws Exception;

    public long getDuplicateCount ();

    public long getWriteCount ();

    public void openInput () throws Exception;

    public boolean publish (SourceProxyRecord item, long phase) throws Exception;

    public void reset () throws IOException;
}