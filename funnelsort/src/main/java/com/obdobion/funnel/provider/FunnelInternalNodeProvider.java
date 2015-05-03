package com.obdobion.funnel.provider;

import java.io.IOException;

import com.obdobion.funnel.Funnel;
import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.FunnelItem;

/**
 * @author Chris DeGreef
 * 
 */
public class FunnelInternalNodeProvider implements FunnelDataProvider
{
    final Funnel     funnel;
    final FunnelItem left;
    final FunnelItem right;

    public FunnelInternalNodeProvider(
            final Funnel _funnel,
            final FunnelItem leftContestantIndex,
            final FunnelItem rightContestantIndex)
    {
        this.funnel = _funnel;
        left = leftContestantIndex;
        right = rightContestantIndex;
    }

    public long actualNumberOfRows ()
    {
        return maximumNumberOfRows();
    }

    public void attachTo (final FunnelItem item)
    {
        item.setProvider(this);
    }

    public void close () throws IOException
    {
        // intentionally empty
    }

    public long maximumNumberOfRows ()
    {
        return Long.MAX_VALUE;
    }

    public boolean next (final FunnelItem item, final long phase) throws IOException
    {
        if (!left.isEndOfData() && left.getData() == null)
            left.next(phase);
        if (!right.isEndOfData() && right.getData() == null)
            right.next(phase);

        if (left.isEndOfData())
        {
            if (right.isEndOfData())
            {
                item.setEndOfData(true);
                return false;
            }
            item.setData(right.getData());
            item.setPhase(phase);
            right.next(phase);
            return true;
        } else if (right.isEndOfData())
        {
            item.setData(left.getData());
            item.setPhase(phase);
            left.next(phase);
            return true;
        }
        /*
         * compare right and left nodes and use the one that is less
         */
        if (left.getData().compareTo(right.getData()) > 0)
        {
            item.setData(right.getData());
            item.setPhase(phase);
            right.next(phase);
        } else
        {
            item.setData(left.getData());
            item.setPhase(phase);
            left.next(phase);
        }

        return true;
    }

    public void reset ()
    {
        // Intentionally empty
    }
}