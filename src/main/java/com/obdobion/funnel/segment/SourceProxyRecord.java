package com.obdobion.funnel.segment;

import java.util.Stack;

import com.obdobion.funnel.parameters.FunnelContext;

/**
 * The internal control record for a specific row in the original source. Any
 * temporary file used in the sort / merge process is formatted with these rows.
 * This allows for a size to be added to increase the performance of random
 * access files. This sortKey is always alphanumeric; it is converted from
 * whatever the fields actually are. This allows the sort to be on a single long
 * string key rather than a conversion to Java objects.
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public class SourceProxyRecord
{
    /**
     * A cache of instances that are not in use at this time. Rather than make
     * new ones all of the time we will attempt to push and pop already created
     * instances from this stack.
     */
    static final public Stack<SourceProxyRecord> AvailableInstances = new Stack<>();

    /**
     * <p>getInstance.</p>
     *
     * @param context a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     * @return a {@link com.obdobion.funnel.segment.SourceProxyRecord} object.
     */
    public static SourceProxyRecord getInstance (final FunnelContext context)
    {
        synchronized (AvailableInstances)
        {
            if (AvailableInstances.isEmpty())
                return new SourceProxyRecord(context);
            final SourceProxyRecord proxy = AvailableInstances.pop();
            proxy.context = context;
            return proxy;
        }
    }

    private FunnelContext context;
    public int            originalInputFileIndex;
    private long          originalRecordNumber;
    public long           originalLocation;
    public int            originalSize;
    public int            size;
    public byte[]         sortKey;

    private SourceProxyRecord(final FunnelContext _context)
    {
        super();
        context = _context;
    }

    /**
     * <p>compareTo.</p>
     *
     * @param o a {@link com.obdobion.funnel.segment.SourceProxyRecord} object.
     * @return a int.
     */
    public int compareTo (final SourceProxyRecord o)
    {
        return compareTo(o, true);
    }

    /**
     * <p>compareTo.</p>
     *
     * @param o a {@link com.obdobion.funnel.segment.SourceProxyRecord} object.
     * @param resolveDuplicates a boolean.
     * @return a int.
     */
    public int compareTo (final SourceProxyRecord o, final boolean resolveDuplicates)
    {
        if (context != null)
            context.comparisonCounter++;

        int unsignedLeft, unsignedRight;
        final int oSize = o.size;
        /*
         * Compare the bytes of the sortkey. Return if they are not equal.
         */
        for (int b = 0; b < size; b++)
        {
            if (b >= oSize)
                return 1;
            unsignedLeft = sortKey[b] & 0xff;
            unsignedRight = o.sortKey[b] & 0xff;
            if (unsignedLeft < unsignedRight)
                return -1;
            if (unsignedLeft > unsignedRight)
                return 1;
        }
        /*
         * They were equal up to the size of the smaller one. If they are not
         * the same size the return.
         */
        final int sizes = size - oSize;
        if (sizes != 0)
            return sizes;

        if (!resolveDuplicates)
            return 0;
        /*
         * The keys were identical in content and size, true dups, so return the
         * first we saw as the winner. If they are both from the same file then
         * return the record number comparison.
         */
        if (originalInputFileIndex == o.originalInputFileIndex)
            return (int) (getOriginalRecordNumber() - o.getOriginalRecordNumber());
        /*
         * Otherwise, finally, compare the file index.
         */
        return (originalInputFileIndex - o.originalInputFileIndex);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals (
        final Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final SourceProxyRecord other = (SourceProxyRecord) obj;
        if (originalInputFileIndex != other.originalInputFileIndex)
            return false;
        if (getOriginalRecordNumber() != other.getOriginalRecordNumber())
            return false;
        return true;
    }

    /**
     * <p>getFunnelContext.</p>
     *
     * @return a {@link com.obdobion.funnel.parameters.FunnelContext} object.
     */
    public FunnelContext getFunnelContext ()
    {
        return context;
    }

    /**
     * <p>Getter for the field <code>originalRecordNumber</code>.</p>
     *
     * @return a long.
     */
    public long getOriginalRecordNumber ()
    {
        return originalRecordNumber;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode ()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + originalInputFileIndex;
        result = prime * result + (int) (getOriginalRecordNumber() ^ (getOriginalRecordNumber() >>> 32));
        return result;
    }

    /**
     * <p>release.</p>
     */
    public void release ()
    {
        synchronized (AvailableInstances)
        {
            AvailableInstances.push(this);
        }
    }

    /**
     * <p>Setter for the field <code>originalRecordNumber</code>.</p>
     *
     * @param p_originalRecordNumber a long.
     */
    public void setOriginalRecordNumber (final long p_originalRecordNumber)
    {
        this.originalRecordNumber = p_originalRecordNumber;
    }

    /** {@inheritDoc} */
    @Override
    public String toString ()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("originalRecordNumber=").append(getOriginalRecordNumber());
        sb.append(" originalLocation=").append(originalLocation);
        sb.append(" originalSize=").append(originalSize);
        sb.append(" sortKey=").append(new String(sortKey).substring(0, size));
        sb.append(" size=").append(size);
        return sb.toString();
    }
}
