/*
 * HistoryImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.history;

import java.util.ArrayList;
import java.util.List;

/**
 * The default implementation of the <code>History<code> interface. 
 *
 * @param <V> the value type
 * 
 * @see com.toolsverse.util.history.History
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class HistoryImpl<V> implements History<V>
{
    
    /** DEFAULT_MAX_SIZE. */
    private static final int DEFAULT_MAX_SIZE = 50;
    
    /** The _history. */
    private List<V> _history;
    
    /** The _max size. */
    private int _maxSize;
    
    /**
     * Instantiates a new history impl using <code>DEFAULT_MAX_SIZE</code>. 
     */
    public HistoryImpl()
    {
        _history = new ArrayList<V>();
        
        _maxSize = DEFAULT_MAX_SIZE;
    }
    
    /**
     * Instantiates a new history impl using specific maximum size.
     *
     * @param maxSize the max size
     */
    public HistoryImpl(int maxSize)
    {
        this();
        
        _maxSize = maxSize;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.history.History#canGoBack(java.lang.Object)
     */
    public boolean canGoBack(V current)
    {
        return current != null && _history.size() > 1
                && _history.indexOf(current) > 0;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.history.History#canGoForward(java.lang.Object)
     */
    public boolean canGoForward(V current)
    {
        return current != null && _history.size() > 1
                && _history.indexOf(current) < _history.size() - 1;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.history.History#clear()
     */
    public void clear()
    {
        _history.clear();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.history.History#contains(java.lang.Object)
     */
    public boolean contains(V element)
    {
        return _history.contains(element);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.history.History#elements()
     */
    public List<V> elements()
    {
        return _history;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.history.History#goBack(java.lang.Object)
     */
    public V goBack(V current)
    {
        if (!canGoBack(current))
            return null;
        
        return _history.get(_history.indexOf(current) - 1);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.history.History#goForward(java.lang.Object)
     */
    public V goForward(V current)
    {
        if (!canGoForward(current))
            return null;
        
        return _history.get(_history.indexOf(current) + 1);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.history.History#remove(java.lang.Object)
     */
    public void remove(V element)
    {
        _history.remove(element);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.history.History#setMaxSize(int)
     */
    public void setMaxSize(int value)
    {
        _maxSize = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.history.History#size()
     */
    public int size()
    {
        return _history.size();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.history.History#visit(java.lang.Object)
     */
    public void visit(V element)
    {
        if (!_history.contains(element))
        {
            _history.add(element);
            
            if (_history.size() > _maxSize)
                _history.remove(0);
        }
    }
}
