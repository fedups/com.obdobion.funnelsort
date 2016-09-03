package com.obdobion.funnel.segment;

import java.io.IOException;
import java.text.ParseException;

import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.FunnelItem;

/**
 * 
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
     * <p>
     * Constructor for Segment.
     * </p>
     *
     * @param _workfile a {@link com.obdobion.funnel.segment.WorkRepository}
     *            object.
     * @throws java.io.IOException if any.
     */
    public Segment(final WorkRepository _workfile) throws IOException
    {
        workfile = _workfile;
        startingPosition = _workfile.outputPosition();
        nextPosition = startingPosition;
        rowsInSegment = 0;
        nextRow = 0;
    }

    /** {@inheritDoc} */
    @Override
    public long actualNumberOfRows()
    {
        return rowsInSegment;
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
        return rowsInSegment;
    }

    /** {@inheritDoc} */
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

    /** {@inheritDoc} */
    @Override
    public void reset()
    {
        // intentionally empty
    }

    /**
     * <p>
     * Setter for the field <code>segmentProvider</code>.
     * </p>
     *
     * @param _segmentProvider a
     *            {@link com.obdobion.funnel.segment.SegmentedPublisherAndProvider}
     *            object.
     */
    public void setSegmentProvider(final SegmentedPublisherAndProvider _segmentProvider)
    {
        segmentProvider = _segmentProvider;
    }

    /**
     * <p>
     * write.
     * </p>
     *
     * @param item a {@link com.obdobion.funnel.segment.SourceProxyRecord}
     *            object.
     * @throws java.io.IOException if any.
     */
    public void write(final SourceProxyRecord item) throws IOException
    {
        workfile.write(item);
        rowsInSegment++;
    }
}
