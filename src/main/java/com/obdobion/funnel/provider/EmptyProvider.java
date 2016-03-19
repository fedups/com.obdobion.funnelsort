package com.obdobion.funnel.provider;

import java.io.IOException;

import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.FunnelItem;

/**
 * @author Chris DeGreef
 * 
 */
public class EmptyProvider implements FunnelDataProvider
{
    public long actualNumberOfRows ()
    {
        return 0;
    }

    public void attachTo (final FunnelItem item)
    {
        // intentionally empty
    }

    public void close () throws IOException
    {
        // intentionally empty
    }

    public long maximumNumberOfRows ()
    {
        return Long.MAX_VALUE;
    }

    public boolean next (final FunnelItem item, final long phase)
    {
        item.setData(null);
        item.setEndOfData(true);
        return false;
    }

    public void reset ()
    {
        // Intentionally empty
    }
}