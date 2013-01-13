/*
 * DataSetRecord.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * The collection of values for one database record. Supports multiple versions of the same cell.
 *
 * @see com.toolsverse.etl.common.DataSetData
 * @see com.toolsverse.etl.common.DataSet
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class DataSetRecord implements Serializable
{
    /**
     * 
     * This class represents a versions of the cell. 
     *
     */
    private static class CellVersions extends ArrayList<Object>
    {
        Object _value;
        
        CellVersions(Object value)
        {
            _value = value;
        }
        
        Object getValue()
        {
            return _value;
        }
    }
    
    /** The record. */
    private final List<Object> _record;
    
    /**
     * Instantiates a new DataSetRecord.
     */
    public DataSetRecord()
    {
        _record = new ArrayList<Object>();
    }
    
    /**
     * Adds the value at the index.
     *
     * @param index the index
     * @param value the value
     */
    public void add(int index, Object value)
    {
        _record.add(index, value);
    }
    
    /**
     * Adds the value.
     *
     * @param value the value
     */
    public void add(Object value)
    {
        _record.add(value);
    }
    
    /**
     * Add version to the cell.
     * 
     * @param index the column number
     * @param value the value to add
     */
    public void addVersion(int index, Object value)
    {
        Object cellValue = _record.get(index);
        
        if (cellValue instanceof CellVersions)
        {
            ((CellVersions)cellValue).add(value);
        }
        else
        {
            CellVersions cell = new CellVersions(cellValue);
            
            cell.add(value);
            
            set(index, cell);
        }
    }
    
    /**
     * Clears.
     */
    public void clear()
    {
        _record.clear();
    }
    
    /**
     * Checks if any version of the cell contains given value. Null is ignored. 
     * @param index the column number
     * @param value the value to check
     * @param ignoreCase if <code>true</code> ignore char case when comparing
     * @param doTrim if <code>true</code> trim before comparing
     * @return <code>true</code> if any version of the cell contains given value
     */
    public boolean contains(int index, Object value, boolean ignoreCase,
            boolean doTrim)
    {
        Object cellValue = _record.get(index);
        
        if (cellValue instanceof CellVersions)
        {
            CellVersions versions = (CellVersions)cellValue;
            
            boolean equals = equals(value, versions.getValue(), ignoreCase,
                    doTrim);
            
            if (equals)
                return true;
            
            for (Object val : versions)
            {
                equals = equals(value, val, ignoreCase, doTrim);
                if (equals)
                    return true;
            }
        }
        else
        {
            return equals(value, cellValue, ignoreCase, doTrim);
        }
        
        return false;
    }
    
    /**
     * Deletes column.
     *
     * @param col the column
     */
    public void deleteCol(int col)
    {
        if (col >= 0 && col < _record.size())
            _record.remove(col);
    }
    
    /**
     * Compares two objects.
     * @param value the value
     * @param toCompare the value to compare with
     * @param ignoreCase if <code>true</code> ignore char case when comparing
     * @param doTrim if <code>true</code> trim before comparing
     * @return true if objects are equal
     */
    private boolean equals(Object value, Object toCompare, boolean ignoreCase,
            boolean doTrim)
    {
        if (value == null && toCompare == null)
            return true;
        
        if (value != null && toCompare == null)
            return false;
        
        if (value == null && toCompare != null)
            return false;
        
        String valueStr = value.toString();
        String compareToStr = toCompare.toString();
        
        valueStr = doTrim ? valueStr.trim() : valueStr;
        
        compareToStr = doTrim ? compareToStr.trim() : compareToStr;
        
        return ignoreCase ? valueStr.equalsIgnoreCase(compareToStr) : valueStr
                .equals(compareToStr);
    }
    
    /**
     * Gets the value at the index.
     *
     * @param index the index
     * @return the object
     */
    public Object get(int index)
    {
        Object value = _record.get(index);
        
        if (value instanceof CellVersions)
            return ((CellVersions)value).getValue();
        
        return value;
    }
    
    /**
     * Calculates the index of the version using given value.
     *  
     * @param index the column
     * @param value the value
     * @param ignoreCase if <code>true</code> ignore char case when comparing
     * @param doTrim if <code>true</code> trim before comparing
     * @return the index of the version using given value
     */
    public int getIndexOfVersion(int index, Object value, boolean ignoreCase,
            boolean doTrim)
    {
        Object cellValue = _record.get(index);
        
        if (cellValue instanceof CellVersions)
        {
            CellVersions versions = (CellVersions)cellValue;
            
            boolean equals = equals(value, versions.getValue(), ignoreCase,
                    doTrim);
            
            if (equals)
                return 0;
            
            int ind = 1;
            for (Object val : versions)
            {
                equals = equals(value, val, ignoreCase, doTrim);
                if (equals)
                    return ind;
                
                ind++;
            }
        }
        else
        {
            if (equals(value, cellValue, ignoreCase, doTrim))
                return 0;
        }
        
        return -1;
    }
    
    /**
     * Gets number of versions for the given cell index 
     * @param index the cell index
     * @return number of versions
     */
    public int getNumberOfVersions(int index)
    {
        Object cellValue = _record.get(index);
        
        if (cellValue instanceof CellVersions)
        {
            return ((CellVersions)cellValue).size() + 1;
        }
        else
        {
            return 1;
        }
        
    }
    
    /**
     * Get version of the cell value
     * 
     * @param index the index of the cell
     * @param version the version number. Starts from 0.
     * @return version of the cell value
     */
    public Object getVersion(int index, int version)
    {
        Object cellValue = _record.get(index);
        
        if (cellValue instanceof CellVersions)
        {
            CellVersions versions = (CellVersions)cellValue;
            
            if (version == 0)
                return versions.getValue();
            if (version < 0 || version > versions.size())
                return null;
            else
                return versions.get(version - 1);
        }
        else
        {
            if (version == 0)
                return cellValue;
            else
                return null;
        }
        
    }
    
    /**
     * Removes version of the cell
     * @param index the index of the cell
     * @param version the version
     */
    public void removeVersion(int index, int version)
    {
        Object cellValue = _record.get(index);
        
        if (cellValue instanceof CellVersions)
        {
            CellVersions versions = (CellVersions)cellValue;
            
            if (version < 0 || version > versions.size())
                return;
            else
                versions.remove(version - 1);
        }
    }
    
    /**
     * Sets the value at the index.
     *
     * @param index the index
     * @param value the value
     */
    public void set(int index, Object value)
    {
        _record.set(index, value);
    }
    
    /**
     * Sets the value for the version.
     * 
     * @param index the column
     * @param version the version number
     * @param value the value
     */
    public void setVersion(int index, int version, Object value)
    {
        Object cellValue = _record.get(index);
        
        if (cellValue instanceof CellVersions)
        {
            ((CellVersions)cellValue).set(version, value);
        }
        
    }
    
    /**
     * Gets the number of fields.
     *
     * @return the number of fields
     */
    public int size()
    {
        return _record.size();
    }
}
