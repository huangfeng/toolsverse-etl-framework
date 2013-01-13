/*
 * TypedKeyValue.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.io.Serializable;

/**
 * Generic key-value class.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public class TypedKeyValue<K, V> implements Serializable
{
    
    /** The _key. */
    private K _key = null;
    
    /** The _value. */
    private V _value = null;
    
    /**
     * Instantiates a new key-value pair.
     * 
     * @param key The key
     * @param value The value
     */
    public TypedKeyValue(K key, V value)
    {
        _key = key;
        _value = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object compareTo)
    {
        if (!(compareTo instanceof TypedKeyValue))
            return false;
        
        return toString().equals(compareTo.toString());
    }
    
    /**
     * Gets the key.
     * 
     * @return the key
     */
    public K getKey()
    {
        return _key;
    }
    
    /**
     * Gets the value.
     * 
     * @return the value
     */
    public V getValue()
    {
        return _value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return toString().hashCode();
    }
    
    /**
     * Sets the key.
     * 
     * @param key
     *            The new key
     */
    public void setKey(K key)
    {
        _key = key;
    }
    
    /**
     * Sets the value.
     * 
     * @param value
     *            The new value
     */
    public void setValue(V value)
    {
        _value = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return Utils.makeString(_key) + " " + Utils.makeString(_value);
    }
}
