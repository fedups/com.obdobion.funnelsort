package com.obdobion.funnel;

import java.io.IOException;

/**
 * @author Chris DeGreef
 * 
 */
public interface FunnelDataProvider
{
    public long actualNumberOfRows ();

    public void attachTo (FunnelItem item);

    public void close () throws IOException;

    public long maximumNumberOfRows ();

    public boolean next (FunnelItem item, long phase) throws IOException;

    public void reset () throws IOException;
}