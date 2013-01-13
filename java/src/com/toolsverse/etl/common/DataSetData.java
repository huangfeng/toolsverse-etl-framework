/*
 * DataSetData.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * The collection of the DataSetRecords used by DataSet to store data.
 *
 * @see com.toolsverse.etl.common.DataSet
 * @see com.toolsverse.etl.common.DataSetRecord
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class DataSetData implements Serializable
{
    /** Sort types */
    public static enum SortType
    {
        ASC, DESC,
    }
    
    /** The data. */
    private List<DataSetRecord> _data;
    
    /**
     * Instantiates a new DataSetData.
     */
    public DataSetData()
    {
        _data = new ArrayList<DataSetRecord>();
    }
    
    /**
     * Adds the record.
     *
     * @param value the record
     */
    public void add(DataSetRecord value)
    {
        _data.add(value);
    }
    
    /**
     * Adds the record at the index.
     *
     * @param index the index
     * @param value the record
     */
    public void add(int index, DataSetRecord value)
    {
        _data.add(index, value);
    }
    
    /**
     * Clears data
     */
    public void clear()
    {
        _data.clear();
    }
    
    /**
     * Deletes record at the index.
     *
     * @param index the index of the record
     * 
     * @return deleted record
     */
    public DataSetRecord delete(int index)
    {
        return _data.remove(index);
    }
    
    /**
     * Gets the record for the index.
     *
     * @param index the index
     * @return the record
     */
    public DataSetRecord get(int index)
    {
        return _data.get(index);
    }
    
    /**
     * Checks if it is empty.
     *
     * @return true, if is empty
     */
    public boolean isEmpty()
    {
        return _data.isEmpty();
    }
    
    /**
     * Sets the record at the index.
     *
     * @param index the index
     * @param value the record
     */
    public void set(int index, DataSetRecord value)
    {
        _data.set(index, value);
    }
    
    /**
     * Gets the number of records.
     *
     * @return the number of records
     */
    public int size()
    {
        return _data.size();
    }
    
    /** Sorts data using given order by list with field indexes.
     *  
     * @param orderBy the order by list
    */
    public void sort(final List<TypedKeyValue<Integer, SortType>> orderBy)
    {
        if (_data.size() == 0 || orderBy == null || orderBy.size() == 0)
            return;
        
        Collections.sort(_data, new Comparator<DataSetRecord>()
        {
            public int compare(DataSetRecord r1, DataSetRecord r2)
            {
                int ret;
                
                for (TypedKeyValue<Integer, SortType> item : orderBy)
                {
                    Object v1 = r1.get(item.getKey());
                    Object v2 = r2.get(item.getKey());
                    
                    ret = Utils.compareTo(v1, v2);
                    if (ret == 0)
                        continue;
                    
                    return item.getValue() == SortType.ASC ? ret : ret * -1;
                }
                
                return 0;
            }
        });
    }
}
