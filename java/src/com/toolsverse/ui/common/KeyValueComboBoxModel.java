/*
 * KeyValueComboBoxModel.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.DefaultComboBoxModel;

import com.toolsverse.util.KeyValue;
import com.toolsverse.util.Utils;

/**
 * The DefaultComboBoxModel where display values are different from the return
 * values. Display and Return value can be any object.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

@SuppressWarnings("rawtypes")
public class KeyValueComboBoxModel extends DefaultComboBoxModel
{
    
    /**
     * The Class CbKeyValue. Uses Utils.makeString(getValue()) to return a value
     * displayed in the combo box.
     */
    public class CbKeyValue extends KeyValue
    {
        
        /**
         * Instantiates a new CbKeyValue.
         * 
         * @param keyValue
         *            the KeyValue
         */
        public CbKeyValue(KeyValue keyValue)
        {
            super(keyValue.getKey(), keyValue.getValue());
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.toolsverse.util.TypedKeyValue#toString()
         */
        @Override
        public String toString()
        {
            return Utils.makeString(getValue());
        }
    }
    
    /** The keys. */
    private final Map<Object, Object> _keys;
    
    /** The values. */
    private final Map<Object, Object> _values;
    
    /**
     * Instantiates a new KeyValueComboBoxModel.
     */
    public KeyValueComboBoxModel()
    {
        _keys = new HashMap<Object, Object>();
        _values = new HashMap<Object, Object>();
    }
    
    /**
     * Instantiates a new KeyValueComboBoxModel using given KeyValue list. Keys
     * are "return values", values are "displayed values".
     * 
     * @param data
     *            the KeyValue list
     */
    public KeyValueComboBoxModel(List<KeyValue> data)
    {
        this(null, data);
    }
    
    /**
     * Instantiates a new KeyValueComboBoxModel using given ListProvider.
     * 
     * @param listProvider
     *            the list provider
     */
    public KeyValueComboBoxModel(ListProvider listProvider)
    {
        this(listProvider, null);
    }
    
    /**
     * Instantiates a new KeyValueComboBoxModel using given ListProvider and
     * KeyValue list. Basically one of these must be not null. If both are not
     * null and not empty the objects from the KeyValue list will be added to
     * the list returned by list provider.
     * 
     * @param listProvider
     *            the list provider
     * @param data
     *            the KeyValue list
     */
    public KeyValueComboBoxModel(ListProvider listProvider, List<KeyValue> data)
    {
        this();
        
        init(listProvider, data);
    }
    
    /**
     * Gets the KeyValue by key (key is a returned value).
     * 
     * @param key
     *            the key
     * @return the KeyValue
     */
    public KeyValue getItemByKey(Object key)
    {
        int itemCount = getSize();
        for (int i = 0; i < itemCount; i++)
        {
            KeyValue keyValue = (KeyValue)getElementAt(i);
            
            if (keyValue.getKey().equals(key))
                return keyValue;
        }
        
        return null;
    }
    
    /**
     * Gets the key at the position index.
     * 
     * @param index
     *            the index
     * @return the key (key is a returned value)
     */
    public Object getKeyAt(int index)
    {
        return index >= 0 && getSize() > 0 && index < getSize() ? ((KeyValue)getElementAt(index))
                .getKey() : null;
    }
    
    /**
     * Gets the key by value. Key is a returned value, value is a displayed
     * value.
     * 
     * @param value
     *            the value
     * @return the key by value
     */
    public Object getKeyByValue(Object value)
    {
        return _values.get(value);
    }
    
    /**
     * Gets the value by key. Key is a returned value, value is a displayed
     * value.
     * 
     * @param key
     *            the key
     * @return the value by key
     */
    public Object getValueByKey(Object key)
    {
        return _keys.get(key);
    }
    
    /**
     * Initializes model using given ListProvider and KeyValue list. Basically
     * one of these must be not null. If both are not null and not empty the
     * objects from the KeyValue list will be added to the list returned by list
     * provider.
     * 
     * @param listProvider
     *            the list provider
     * @param data
     *            the KeyValue list
     */
    @SuppressWarnings("unchecked")
    public void init(ListProvider listProvider, List<KeyValue> data)
    {
        if (listProvider != null)
            data = listProvider.getList();
        
        if (data == null || data.size() == 0)
            return;
        
        for (KeyValue keyValue : data)
        {
            addElement(new CbKeyValue(keyValue));
            
            _keys.put(keyValue.getKey(), keyValue.getValue());
            _values.put(keyValue.getValue(), keyValue.getKey());
        }
    }
}
