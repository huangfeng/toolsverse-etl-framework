/*
 * ListHashMap.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * The HashMap where all values additionally stored in the List. Compare to the ListHashMap allows any order 
 * of the entries in the list. 
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public class ListHashMap<K, V> extends HashMap<K, V>
{
    
    /** The _list. */
    private List<V> _list;
    
    /**
     * Instantiates a new empty ListHashMap.
     */
    public ListHashMap()
    {
        _list = createList();
    }
    
    /**
     * Associates the specified value with the map.size + 1 in this map and add value to the end of the list.
     * 
     * @param value the value to add
     * 
     * @return true, if successful
     */
    @SuppressWarnings("unchecked")
    public boolean add(V value)
    {
        super.put((K)(String.valueOf(size() + 1)), value);
        
        return _list.add(value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.HashMap#clear()
     */
    @Override
    public void clear()
    {
        super.clear();
        
        _list.clear();
    }
    
    /**
     * Creates the list.
     * 
     * @return the List of values
     */
    public List<V> createList()
    {
        return new ArrayList<V>();
    }
    
    /**
     * Returns the element at the specified position in the list.
     *
     * @param index index of the element to return
     * @return the element at the specified position in this list
     * @throws IndexOutOfBoundsException if the index is out of range
     *         (<tt>index &lt; 0 || index &gt;= size()</tt>)
     */
    public V get(int index)
    {
        return _list.get(index);
    }
    
    /**
     * Gets the list.
     * 
     * @return the list of values
     */
    public List<V> getList()
    {
        return _list;
    }
    
    /**
     * Returns the index of the first occurrence of the specified element
     * in this list, or -1 if this list does not contain the element.
     * More formally, returns the lowest index <tt>i</tt> such that
     * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
     * or -1 if there is no such index.
     *
     * @param value element to search for
     * @return the index of the first occurrence of the specified element in
     *         this list, or -1 if this list does not contain the element
     * @throws ClassCastException if the type of the specified element
     *         is incompatible with this list (optional)
     * @throws NullPointerException if the specified element is null and this
     *         list does not permit null elements (optional)
     */
    public int indexOf(V value)
    {
        return _list.indexOf(value);
    }
    
    /**
     * Associates the specified value with the specified key in this map
     * and adds value to the end of the list. 
     * If the map previously contained a mapping for the key, the old value is replaced by the specified value.  (A map
     * <tt>m</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(Object) m.containsKey(k)} would return
     * <tt>true</tt>.)
     * 
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with <tt>key</tt>,
     *         if the implementation supports <tt>null</tt> values.)
     * @throws UnsupportedOperationException if the <tt>put</tt> operation
     *         is not supported by this map
     * @throws ClassCastException if the class of the specified key or value
     *         prevents it from being stored in this map
     * @throws NullPointerException if the specified key or value is null
     *         and this map does not permit null keys or values
     * @throws IllegalArgumentException if some property of the specified key
     *         or value prevents it from being stored in this map
     */
    @Override
    public V put(K key, V value)
    {
        _list.add(value);
        
        return super.put(key, value);
    }
    
    /**
     * Associates the specified value with the specified key in this map
     * and adds value to the list at the specified index. 
     * If the map previously contained a mapping for the key, the old value is replaced by the specified value.  (A map
     * <tt>m</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(Object) m.containsKey(k)} would return
     * <tt>true</tt>.)
     * 
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param index the index of the value in the list 
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with <tt>key</tt>,
     *         if the implementation supports <tt>null</tt> values.)
     * @throws UnsupportedOperationException if the <tt>put</tt> operation
     *         is not supported by this map
     * @throws ClassCastException if the class of the specified key or value
     *         prevents it from being stored in this map
     * @throws NullPointerException if the specified key or value is null
     *         and this map does not permit null keys or values
     * @throws IllegalArgumentException if some property of the specified key
     *         or value prevents it from being stored in this map
     */
    public V put(K key, V value, int index)
    {
        _list.add(index, value);
        
        return super.put(key, value);
    }
    
    /**
     * Associates the specified value with the specified key in this map
     * and adds key to the end of the list. 
     * If the map previously contained a mapping for the key, the old value is replaced by the specified value.  (A map
     * <tt>m</tt> is said to contain a mapping for a key <tt>k</tt> if and only
     * if {@link #containsKey(Object) m.containsKey(k)} would return
     * <tt>true</tt>.)
     * 
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     *         (A <tt>null</tt> return can also indicate that the map
     *         previously associated <tt>null</tt> with <tt>key</tt>,
     *         if the implementation supports <tt>null</tt> values.)
     * @throws UnsupportedOperationException if the <tt>put</tt> operation
     *         is not supported by this map
     * @throws ClassCastException if the class of the specified key or value
     *         prevents it from being stored in this map
     * @throws NullPointerException if the specified key or value is null
     *         and this map does not permit null keys or values
     * @throws IllegalArgumentException if some property of the specified key
     *         or value prevents it from being stored in this map
     */
    @SuppressWarnings("unchecked")
    public V putKey(K key, V value)
    {
        V rtn = super.put(key, value);
        
        _list.add((V)key);
        
        return rtn;
    }
    
    /**
     * Removes the mapping for a key from this map and from the list if it is present.
     * More formally, if this map contains a mapping
     * from key <tt>k</tt> to value <tt>v</tt> such that
     * <code>(key==null ?  k==null : key.equals(k))</code>, that mapping
     * is removed.  (The map can contain at most one such mapping.)
     *
     * <p>Returns the value to which this map previously associated the key,
     * or <tt>null</tt> if the map contained no mapping for the key.
     *
     * <p>If this map permits null values, then a return value of
     * <tt>null</tt> does not <i>necessarily</i> indicate that the map
     * contained no mapping for the key; it's also possible that the map
     * explicitly mapped the key to <tt>null</tt>.
     *
     * <p>The map will not contain a mapping for the specified key once the
     * call returns.
     *
     * @param key key whose mapping is to be removed from the map
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     * @throws UnsupportedOperationException if the <tt>remove</tt> operation
     *         is not supported by this map
     * @throws ClassCastException if the key is of an inappropriate type for
     *         this map (optional)
     * @throws NullPointerException if the specified key is null and this
     *         map does not permit null keys (optional)
     */
    @Override
    public V remove(Object key)
    {
        Object value = get(key);
        
        if (value != null)
            _list.remove(value);
        
        return super.remove(key);
    }
    
    /**
     * If value associated with the key doesn't (or equal to null) associates the specified value 
     * with the specified key in this map and adds value to the end of the list.
     * Otherwise depending on the argument <code>replace</code> either returns old value associated with the key 
     * (if replace == false) or associates the specified value with the key and replaces value in the list at the 
     * index of the previous value in the specified value.       
     * 
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @param replace if <code>true</code> replaces existing association on the new one 
     * @return the value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>.
     */
    public V set(K key, V value, boolean replace)
    {
        V old = get(key);
        
        if (old == null)
            return put(key, value);
        
        if (!replace)
            return old;
        
        int index = _list.indexOf(old);
        
        V rtn = super.put(key, value);
        
        _list.set(index, value);
        
        return rtn;
    }
}
