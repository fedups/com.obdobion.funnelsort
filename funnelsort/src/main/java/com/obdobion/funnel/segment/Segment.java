package com.obdobion.funnel.segment;

import java.io.IOException;

import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.FunnelItem;

/**
 * @author Chris DeGreef
 * 
 */
class Segment implements FunnelDataProvider
{
    SegmentedPublisherAndProvider segmentProvider;
    final WorkRepository          workfile;
    long                          startingPosition;
    long                          rowsInSegment;
    long                          nextPosition;
    long                          nextRow;

    /**
     * @param _workfile
     * @throws IOException
     */
    public Segment(final WorkRepository _workfile) throws IOException
    {
        this.workfile = _workfile;
        this.startingPosition = _workfile.outputPosition();
        this.nextPosition = startingPosition;
        this.rowsInSegment = 0;
        this.nextRow = 0;
    }

    public long actualNumberOfRows ()
    {
        return rowsInSegment;
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
        return rowsInSegment;
    }

    public boolean next (final FunnelItem item, final long phase) throws IOException
    {
        if (nextRow >= rowsInSegment)
        {
            /*
             * Only return 1 complete segment per phase.
             */
            if (item.getPhase() == phase)
            {
                item.setEndOfData(true);
                return false;
            }
            item.setPhase(phase);

            segmentProvider.attachTo(item);
            return item.next(phase);
        }

        /*
         * A new wrapper that gets passed around in the funnel.
         */
        item.setData(SourceProxyRecord.getInstance());
        nextPosition += workfile.read(nextPosition, item.getData());
        nextRow++;

        item.setPhase(phase);

        return true;
    }

    public void setSegmentProvider (final SegmentedPublisherAndProvider _segmentProvider)
    {
        this.segmentProvider = _segmentProvider;
    }

    public void write (final SourceProxyRecord item) throws IOException
    {
        workfile.write(item);
        rowsInSegment++;
    }

    public void reset ()
    {
        // intentionally empty
    }
}