package com.obdobion.funnel.provider;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.Funnel;
import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.FunnelItem;

/**
 * <p>FunnelInternalNodeProvider class.</p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class FunnelInternalNodeProvider implements FunnelDataProvider
{
    final Funnel     funnel;
    final FunnelItem left;
    final FunnelItem right;

    /**
     * <p>Constructor for FunnelInternalNodeProvider.</p>
     *
     * @param _funnel a {@link com.obdobion.funnel.Funnel} object.
     * @param leftContestantIndex a {@link com.obdobion.funnel.FunnelItem} object.
     * @param rightContestantIndex a {@link com.obdobion.funnel.FunnelItem} object.
     */
    public FunnelInternalNodeProvider(
            final Funnel _funnel,
            final FunnelItem leftContestantIndex,
            final FunnelItem rightContestantIndex)
    {
        funnel = _funnel;
        left = leftContestantIndex;
        right = rightContestantIndex;
    }

    /** {@inheritDoc} */
    @Override
    public long actualNumberOfRows()
    {
        return maximumNumberOfRows();
    }

    /** {@inheritDoc} */
    @Override
    public void attachTo(final FunnelItem item)
    {
        item.setProvider(this);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException
    {
        // intentionally empty
    }

    /** {@inheritDoc} */
    @Override
    public long maximumNumberOfRows()
    {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override
    public boolean next(final FunnelItem item, final long phase) throws IOException, ParseException
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

    /** {@inheritDoc} */
    @Override
    public void reset()
    {
        // Intentionally empty
    }
}
