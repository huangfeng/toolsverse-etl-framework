/*
 * UpdateArrayList.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * ArrayList implementation of the UpdateList interface. The list can track only deletes or deletes and inserts.
 * Also the list can support "baseline all" functionality when <code>com.toolsverse.util.Mutable#baseline()</code>
 * call will baseline all objects in the list as well. In this case all objects must implement <code>com.toolsverse.util.Mutable</code>
 * interface.
 *
 * @see com.toolsverse.util.Mutable
 *
 * @param <E> the element type
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public class UpdateArrayList<E> extends ArrayList<E> implements UpdateList<E>
{
    
    /** The inserts. */
    private List<E> _inserts;
    
    /** The deletes. */
    private List<E> _deletes;
    
    /** The "only deletes" flag. */
    private boolean _onlyDeletes;
    
    /** The "is dirty" flag. */
    private boolean _isDirty;
    
    /** The _baseline all. */
    private boolean _baselineAll;
    
    /**
     * Instantiates a new empty UpdateArrayList. Inserts and deletes will be tracked. List will support
     * baseline all. 
     */
    public UpdateArrayList()
    {
        this(false);
    }
    
    /**
     * Instantiates a new empty UpdateArrayList. If <code>onlyDeletes == true</code> the list will 
     * track only deletes. List will support baseline all. 
     * 
     * @param onlyDeletes if <code>true</code> the list will track only deletes 
     */
    public UpdateArrayList(boolean onlyDeletes)
    {
        this(onlyDeletes, true);
    }
    
    /**
     * Instantiates a new empty UpdateArrayList. If <code>onlyDeletes == true</code> the list will 
     * track only deletes. List will support baseline all functionality if argument 
     * <code>baselineAll == true</code>.   
     * 
     * @param onlyDeletes if <code>true</code> the list will track only deletes
     * @param baselineAll if <code>true</code> the list will support baseline all
     */
    public UpdateArrayList(boolean onlyDeletes, boolean baselineAll)
    {
        _inserts = new ArrayList<E>();
        _deletes = new ArrayList<E>();
        _onlyDeletes = onlyDeletes;
        _isDirty = false;
        _baselineAll = baselineAll;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.ArrayList#add(java.lang.Object)
     */
    @Override
    public boolean add(E value)
    {
        if (!_onlyDeletes)
            addInsert(value);
        
        setDirty(true);
        
        return super.add(value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.ArrayList#add(int, java.lang.Object)
     */
    @Override
    public void add(int index, E value)
    {
        super.add(index, value);
        
        setDirty(true);
        
        if (!_onlyDeletes)
            addInsert(value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.ArrayList#addAll(java.util.Collection)
     */
    @Override
    public boolean addAll(Collection<? extends E> c)
    {
        if (!_onlyDeletes)
            for (E value : c)
            {
                addInsert(value);
            }
        
        setDirty(true);
        
        return super.addAll(c);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.ArrayList#addAll(int, java.util.Collection)
     */
    @Override
    public boolean addAll(int index, Collection<? extends E> c)
    {
        if (!_onlyDeletes)
            for (E value : c)
            {
                addInsert(value);
            }
        
        setDirty(true);
        
        return super.addAll(index, c);
    }
    
    /**
     * Adds the element to the list deleted elements.
     * 
     * @param value the element to add to the list deleted elements
     */
    private void addDelete(E value)
    {
        if (value != null && _deletes.indexOf(value) < 0)
            _deletes.add(value);
    }
    
    /**
     * Adds the element to the list inserted elements.
     * 
     * @param value the element to add to the list inserted elements
     */
    private void addInsert(E value)
    {
        if (value != null && _inserts.indexOf(value) < 0)
            _inserts.add(value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.Mutable#baseline()
     */
    public void baseline()
    {
        _inserts.clear();
        _deletes.clear();
        
        if (_baselineAll)
            for (E value : this)
            {
                if (value instanceof Mutable)
                    ((Mutable)value).baseline();
            }
        
        setDirty(false);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.ArrayList#clear()
     */
    @Override
    public void clear()
    {
        super.clear();
        _inserts.clear();
        _deletes.clear();
        setDirty(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.UpdateList#getDeletes()
     */
    public List<E> getDeletes()
    {
        return _deletes;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.UpdateList#getInserts()
     */
    public List<E> getInserts()
    {
        return _inserts;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.Mutable#isDirty()
     */
    public boolean isDirty()
    {
        return _isDirty;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.ArrayList#remove(int)
     */
    @Override
    public E remove(int index)
    {
        E value = super.remove(index);
        
        addDelete(value);
        
        setDirty(true);
        
        return value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.ArrayList#remove(java.lang.Object)
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean remove(Object value)
    {
        addDelete((E)value);
        
        setDirty(true);
        
        return super.remove(value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.AbstractCollection#removeAll(java.util.Collection)
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean removeAll(Collection<?> c)
    {
        if (c != null)
            for (Object value : c)
            {
                addDelete((E)value);
            }
        
        setDirty(true);
        
        return super.removeAll(c);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.Mutable#setDirty(boolean)
     */
    public void setDirty(boolean value)
    {
        _isDirty = value;
    }
}
