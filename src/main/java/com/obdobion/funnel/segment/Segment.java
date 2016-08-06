package com.obdobion.funnel.segment;

import java.io.IOException;
import java.text.ParseException;

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
        workfile = _workfile;
        startingPosition = _workfile.outputPosition();
        nextPosition = startingPosition;
        rowsInSegment = 0;
        nextRow = 0;
    }

    @Override
    public long actualNumberOfRows()
    {
        return rowsInSegment;
    }

    @Override
    public void attachTo(final FunnelItem item)
    {
        item.setProvider(this);
    }

    @Override
    public void close() throws IOException
    {
        // intentionally empty
    }

    @Override
    public long maximumNumberOfRows()
    {
        return rowsInSegment;
    }

    @Override
    public boolean next(final FunnelItem item, final long phase) throws IOException, ParseException
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
        item.setData(SourceProxyRecord.getInstance(workfile.getContext()));
        nextPosition += workfile.read(nextPosition, item.getData());
        nextRow++;

        item.setPhase(phase);

        return true;
    }

    @Override
    public void reset()
    {
        // intentionally empty
    }

    public void setSegmentProvider(final SegmentedPublisherAndProvider _segmentProvider)
    {
        segmentProvider = _segmentProvider;
    }

    public void write(final SourceProxyRecord item) throws IOException
    {
        workfile.write(item);
        rowsInSegment++;
    }
}