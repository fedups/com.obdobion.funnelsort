package com.obdobion.funnel.orderby;

/**
 * <p>
 * KeyType class.
 * </p>
 *
 * @author Chris DeGreef fedupforone@gmail.com
 */
public enum KeyType
{
    String(AlphaKey.class),
    Integer(DisplayIntKey.class),
    Float(DisplayFloatKey.class),
    BInteger(BinaryIntKey.class),
    BFloat(BinaryFloatKey.class),
    Date(DateKey.class),
    Byte(ByteKey.class),
    Filler(Filler.class);

    /**
     * <p>
     * create.
     * </p>
     *
     * @param keyType a {@link java.lang.String} object.
     * @return a {@link com.obdobion.funnel.orderby.KeyPart} object.
     * @throws java.lang.Exception if any.
     */
    static public KeyPart create(final String keyType) throws Exception
    {
        final KeyType kt = KeyType.valueOf(keyType);
        if (kt == null)
            throw new Exception("\"" + keyType + "\" is not a value key type");
        return (KeyPart) kt.instanceClass.newInstance();
    }

    public Class<?> instanceClass;

    private KeyType(final Class<?> _instanceClass)
    {
        this.instanceClass = _instanceClass;
    }
}
