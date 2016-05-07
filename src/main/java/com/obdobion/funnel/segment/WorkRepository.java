package com.obdobion.funnel.segment;

import java.io.IOException;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * @author Chris DeGreef
 * 
 */
public interface WorkRepository
{
    public abstract void close () throws IOException;

    public abstract void delete () throws IOException;

    public FunnelContext getContext ();

    public abstract void open () throws IOException;

    public abstract long outputPosition ();

    public abstract long read (final long position, final SourceProxyRecord rec) throws IOException;

    public abstract long write (SourceProxyRecord rec) throws IOException;
}