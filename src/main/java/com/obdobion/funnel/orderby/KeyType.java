package com.obdobion.funnel.orderby;

/**
 * @author Chris DeGreef
 *
 */
public enum KeyType
{
        String(AlphaKey.class),
        Integer(DisplayIntKey.class),
        Float(DisplayFloatKey.class),
        BInteger(BinaryIntKey.class),
        BFloat(BinaryFloatKey.class),
        Date(DateKey.class),
        Byte(ByteKey.class);

    static public KeyPart create (final String keyType) throws Exception
    {
        final KeyType kt = KeyType.valueOf(keyType);
        if (kt == null)
            throw new Exception("\"" + keyType + "\" is not a value key type");
        return (KeyPart) kt.instanceClass.newInstance();
    }

    Class<?> instanceClass;

    private KeyType(final Class<?> _instanceClass)
    {
        this.instanceClass = _instanceClass;
    }
}