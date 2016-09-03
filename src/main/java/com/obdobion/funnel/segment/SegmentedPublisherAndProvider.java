package com.obdobion.funnel.segment;

import java.io.IOException;
import java.util.Comparator;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.obdobion.funnel.FunnelDataProvider;
import com.obdobion.funnel.FunnelDataPublisher;
import com.obdobion.funnel.FunnelItem;
import com.obdobion.funnel.parameters.FunnelContext;
import com.obdobion.funnel.provider.EmptyProvider;

/**
 * <p>
 * SegmentedPublisherAndProvider class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class SegmentedPublisherAndProvider implements FunnelDataPublisher, FunnelDataProvider
{
    static final private Logger logger   = LoggerFactory.getLogger(SegmentedPublisherAndProvider.class);

    SourceProxyRecord           previousData;
    boolean                     provider = false;
    Segment                     writingSegment;
    long                        activePhase;
    WorkRepository              workRepository;
    Stack<Segment>              segments;
    long                        actualNumberOfRows;
    private long                writeCount;
    private long                duplicateCount;

    /**
     * <p>
     * Constructor for SegmentedPublisherAndProvider.
     * </p>
     *
     * @param context a {@link com.obdobion.funnel.parameters.FunnelContext}
     *            object.
     * @throws java.io.IOException if any.
     */
    public SegmentedPublisherAndProvider(final FunnelContext context) throws IOException
    {
        /*
         * choose core or file here
         */
        if (context.isCacheWork())
            workRepository = new WorkCore(context);
        else
            workRepository = new WorkFile(context);
    }

    /**
     * <p>
     * actAsProvider.
     * </p>
     */
    public void actAsProvider()
    {
        provider = true;
        logger.trace("switched from publisher to provider");
    }

    /** {@inheritDoc} */
    @Override
    public long actualNumberOfRows()
    {
        return actualNumberOfRows;
    }

    /** {@inheritDoc} */
    @Override
    public void attachTo(final FunnelItem item)
    {
        /*
         * Attach an empty data provider if there are no segments to attach.
         */
        if (segments == null || segments.isEmpty())
        {
            item.setProvider(new EmptyProvider());
            return;
        }
        /*
         * Attach the next segment to this specific top row node in the funnel.
         */
        final Segment segment = segments.pop();
        segment.setSegmentProvider(this);
        segment.attachTo(item);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException
    {
        workRepository.close();
        if (provider)
            workRepository.delete();
    }

    /** {@inheritDoc} */
    @Override
    public long getDuplicateCount()
    {
        return duplicateCount;
    }

    /** {@inheritDoc} */
    @Override
    public long getWriteCount()
    {
        return writeCount;
    }

    /** {@inheritDoc} */
    @Override
    public long maximumNumberOfRows()
    {
        if (segments == null)
            return 0L;
        return segments.size();
    }

    /** {@inheritDoc} */
    @Override
    public boolean next(final FunnelItem item, final long phase)
    {
        /*
         * segments handle this, see the attachTo method for details.
         */
        throw new RuntimeException("not to be called");
    }

    /** {@inheritDoc} */
    @Override
    public void openInput() throws IOException
    {
        if (!provider)
        {
            previousData = null;
            activePhase = -1;
        }
        workRepository.open();
    }

    /** {@inheritDoc} */
    @Override
    public boolean publish(final SourceProxyRecord data, final long phase) throws IOException
    {
        if (activePhase == -1)
            activePhase = phase;
        /*
         * check to see if this item is in order, return false if not.
         */
        if (previousData != null)
            if (previousData.compareTo(data) > 0)
                segment(data, phase);
            else if (writingSegment == null)
                segment(data, phase);
        if (writingSegment == null)
            segment(data, phase);

        if (activePhase != phase)
        {
            logger.trace("segment " + (segments.size() - 1) + " continuing into phase " + phase);
            activePhase = phase;
        }
        writingSegment.write(data);

        /*
         * Return the instance for reuse.
         */
        if (previousData != null)
            previousData.release();

        previousData = data;
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
     * segment.
     * </p>
     *
     * @param data a {@link com.obdobion.funnel.segment.SourceProxyRecord}
     *            object.
     * @param phase a long.
     * @throws java.io.IOException if any.
     */
    public void segment(final SourceProxyRecord data, final long phase) throws IOException
    {
        activePhase = phase;

        if (segments == null)
            segments = new Stack<>();
        writingSegment = new Segment(workRepository);
        segments.push(writingSegment);
        actualNumberOfRows++;

        previousData = null;
    }

    /**
     * <p>
     * setComparator.
     * </p>
     *
     * @param comparator a {@link java.util.Comparator} object.
     */
    public void setComparator(final Comparator<SourceProxyRecord> comparator)
    {
        // intentionally empty
    }
}
