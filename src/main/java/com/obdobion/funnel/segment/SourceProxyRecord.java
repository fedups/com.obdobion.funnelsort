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
 * @author Chris DeGreef
 * 
 */
public class SourceProxyRecord implements Comparable<SourceProxyRecord>
{
    /**
     * A cache of instances that are not in use at this time. Rather than make
     * new ones all of the time we will attempt to push and pop already created
     * instances from this stack.
     */
    static final public Stack<SourceProxyRecord> AvailableInstances = new Stack<>();

    public static SourceProxyRecord getInstance (FunnelContext context)
    {
        synchronized (AvailableInstances)
        {
            if (AvailableInstances.isEmpty())
                return new SourceProxyRecord(context);
            return AvailableInstances.pop();
        }
    }

    private FunnelContext context;
    public int            originalInputFileIndex;
    public long           originalRecordNumber;
    public long           originalLocation;
    public int            originalSize;
    public int            size;
    public byte[]         sortKey;

    private SourceProxyRecord(FunnelContext _context)
    {
        super();
        context = _context;
    }

    public int compareTo (
        final SourceProxyRecord o)
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
        /*
         * The keys were identical in content and size, true dups, so return the
         * first we saw as the winner. If they are both from the same file then
         * return the record number comparison.
         */
        if (originalInputFileIndex == o.originalInputFileIndex)
            return (int) (originalRecordNumber - o.originalRecordNumber);
        /*
         * Otherwise, finally, compare the file index.
         */
        return (originalInputFileIndex - o.originalInputFileIndex);
    }

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
        if (originalRecordNumber != other.originalRecordNumber)
            return false;
        return true;
    }

    @Override
    public int hashCode ()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + originalInputFileIndex;
        result = prime * result + (int) (originalRecordNumber ^ (originalRecordNumber >>> 32));
        return result;
    }

    public void release ()
    {
        synchronized (AvailableInstances)
        {
            AvailableInstances.push(this);
        }
    }

    @Override
    public String toString ()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("originalRecordNumber=").append(originalRecordNumber);
        sb.append(" originalLocation=").append(originalLocation);
        sb.append(" originalSize=").append(originalSize);
        sb.append(" sortKey=").append(new String(sortKey).substring(0, size));
        sb.append(" size=").append(size);
        return sb.toString();
    }
}