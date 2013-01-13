/*
 * StorageManager.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.storage;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.toolsverse.storage.impl.local.LocalStorageProvider;
import com.toolsverse.storage.resource.StorageResource;
import com.toolsverse.storage.util.StorageUtils;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * The singleton class which includes all property management and retrieval
 * related functions. Use it if you need to load\store\get\set\delete
 * properties.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class StorageManager implements StorageProvider, ObjectStorage,
        PersistableStorage
{
    
    /** The instance of the StorageManager. */
    private volatile static StorageManager _instance;
    
    /**
     * Returns an instance of the StorageManager.
     * 
     * @return the storage manager
     */
    public static StorageManager instance()
    {
        if (_instance == null)
        {
            synchronized (StorageManager.class)
            {
                if (_instance == null)
                    _instance = new StorageManager();
            }
        }
        
        return _instance;
    }
    
    /** The storage provider. */
    private StorageProvider _storageProvider;
    
    /**
     * Instantiates a new StorageManager.
     */
    private StorageManager()
    {
        initProvider();
    }
    
    /**
     * Gets the boolean property.
     * 
     * @param key
     *            the key
     * @return the boolean property. If value == null throws
     *         IllegalStateException("Property must have a value") exception.
     */
    public boolean getBooleanProperty(String key)
    {
        Object value = getProperty(key);
        
        if (value == null)
            throw new IllegalStateException("Property must have a value");
        
        if (value instanceof Boolean)
            return ((Boolean)value).booleanValue();
        
        return Utils.str2Boolean(value.toString(), null).booleanValue();
    }
    
    /**
     * Gets the boolean property.
     * 
     * @param key
     *            the key
     * @param def
     *            the default value
     * @return the boolean property
     */
    public boolean getBooleanProperty(String key, boolean def)
    {
        Object value = getProperty(key, def);
        
        if (value == null)
            return def;
        
        if (value instanceof Boolean)
            return ((Boolean)value).booleanValue();
        
        return Utils.str2Boolean(value.toString(), def).booleanValue();
    }
    
    /**
     * Gets the date property.
     * 
     * @param key
     *            the key
     * @param def
     *            the default value
     * @param format
     *            the format
     * @return the date property
     */
    public Date getDateProperty(String key, Date def, String format)
    {
        Object value = getProperty(key, def);
        
        if (value == null)
            return def;
        
        if (value instanceof Date)
            return (Date)value;
        
        return Utils.str2Date(value.toString(), def, format);
    }
    
    /**
     * Gets the date property.
     * 
     * @param key
     *            the key
     * @param format
     *            the format
     * @return the date property. If value == null throws
     *         IllegalStateException("Property must have a value") exception.
     */
    public Date getDateProperty(String key, String format)
    {
        Object value = getProperty(key);
        
        if (value == null)
            throw new IllegalStateException("Property must have a value");
        
        if (value instanceof Date)
            return (Date)value;
        
        return Utils.str2Date(value.toString(), null, format);
    }
    
    /**
     * Gets the double property.
     * 
     * @param key
     *            the key
     * @return the double property. If value == null throws
     *         IllegalStateException("Property must have a value") exception.
     */
    public double getDoubleProperty(String key)
    {
        Object value = getProperty(key);
        
        if (value == null)
            throw new IllegalStateException("Property must have a value");
        
        if (value instanceof Number)
            return ((Number)value).doubleValue();
        
        return Utils.str2Number(value.toString(), null).doubleValue();
    }
    
    /**
     * Gets the double property.
     * 
     * @param key
     *            the key
     * @param def
     *            the default value
     * @return the double property
     */
    public double getDoubleProperty(String key, double def)
    {
        Object value = getProperty(key, def);
        
        if (value == null)
            return def;
        
        if (value instanceof Number)
            return ((Number)value).doubleValue();
        
        return Utils.str2Number(value.toString(), def).doubleValue();
    }
    
    /**
     * Gets the float property.
     * 
     * @param key
     *            the key
     * @return the float property. If value == null throws
     *         IllegalStateException("Property must have a value") exception.
     */
    public float getFloatProperty(String key)
    {
        Object value = getProperty(key);
        
        if (value == null)
            throw new IllegalStateException("Property must have a value");
        
        if (value instanceof Number)
            return ((Number)value).floatValue();
        
        return Utils.str2Number(value.toString(), null).floatValue();
    }
    
    /**
     * Gets the float property.
     * 
     * @param key
     *            the key
     * @param def
     *            the default value
     * @return the float property
     */
    public float getFloatProperty(String key, float def)
    {
        Object value = getProperty(key, def);
        
        if (value == null)
            return def;
        
        if (value instanceof Number)
            return ((Number)value).floatValue();
        
        return Utils.str2Number(value.toString(), def).floatValue();
    }
    
    /**
     * Gets the int property.
     * 
     * @param key
     *            the key
     * @return the int property, If value == null throws
     *         IllegalStateException("Property must have a value") exception.
     */
    public int getIntProperty(String key)
    {
        Object value = getProperty(key);
        
        if (value == null)
            throw new IllegalStateException("Property must have a value");
        
        if (value instanceof Number)
            return ((Number)value).intValue();
        
        return Utils.str2Number(value.toString(), null).intValue();
    }
    
    /**
     * Gets the int property.
     * 
     * @param key
     *            the key
     * @param def
     *            the default value
     * @return the int property
     */
    public int getIntProperty(String key, int def)
    {
        Object value = getProperty(key, def);
        
        if (value == null)
            return def;
        
        if (value instanceof Number)
            return ((Number)value).intValue();
        
        return Utils.str2Number(value.toString(), def).intValue();
    }
    
    /**
     * Gets the long property.
     * 
     * @param key
     *            the key
     * @return the long property. If value == null throws
     *         IllegalStateException("Property must have a value") exception.
     */
    public long getLongProperty(String key)
    {
        Object value = getProperty(key);
        
        if (value == null)
            throw new IllegalStateException("Property must have a value");
        
        if (value instanceof Number)
            return ((Number)value).longValue();
        
        return Utils.str2Number(value.toString(), null).longValue();
    }
    
    /**
     * Gets the long property.
     * 
     * @param key
     *            the key
     * @param def
     *            the default value
     * @return the long property
     */
    public long getLongProperty(String key, long def)
    {
        Object value = getProperty(key, def);
        
        if (value == null)
            return def;
        
        if (value instanceof Number)
            return ((Number)value).longValue();
        
        return Utils.str2Number(value.toString(), def).longValue();
    }
    
    /**
     * Gets the number property.
     * 
     * @param key
     *            the key
     * @return the number property
     */
    public Number getNumberProperty(String key)
    {
        Object value = getProperty(key);
        
        if (value == null)
            return null;
        
        if (value instanceof Number)
            return (Number)value;
        
        return Utils.str2Number(value.toString(), null);
    }
    
    /**
     * Gets the number property.
     * 
     * @param key
     *            the key
     * @param def
     *            the default value
     * @return the number property
     */
    public Number getNumberProperty(String key, Number def)
    {
        Object value = getProperty(key, def);
        
        if (value == null)
            return def;
        
        if (value instanceof Number)
            return (Number)value;
        
        return Utils.str2Number(value.toString(), def);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.StorageProvider#getProperties()
     */
    public Map<String, StorageObject> getProperties()
    {
        if (_storageProvider == null)
            initProvider();
        
        return _storageProvider.getProperties();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.StorageProvider#getProperty(java.lang.String)
     */
    public Object getProperty(String key)
    {
        if (_storageProvider == null)
        {
            initProvider();
            
            if (_storageProvider == null)
                return null;
        }
        
        return _storageProvider.getProperty(key);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.StorageProvider#getProperty(java.lang.String,
     * java.lang.Object)
     */
    public Object getProperty(String key, Object def)
    {
        if (_storageProvider == null)
        {
            initProvider();
            
            if (_storageProvider == null)
                return def;
            else
                return _storageProvider.getProperty(key, def);
        }
        else
            return _storageProvider.getProperty(key, def);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#getString(java.lang.String)
     */
    public String getString(String key)
    {
        Object value = getProperty(key);
        
        return value != null ? value.toString() : null;
    }
    
    /**
     * Gets the string property.
     * 
     * @param key
     *            the key
     * @return the string property
     */
    public String getStringProperty(String key)
    {
        Object value = getProperty(key);
        
        if (value == null)
            return null;
        
        return value.toString();
    }
    
    /**
     * Gets the string property.
     * 
     * @param key
     *            the key
     * @param def
     *            the default value
     * @return the string property
     */
    public String getStringProperty(String key, String def)
    {
        Object value = getProperty(key, def);
        
        if (value == null)
            return def;
        
        return value.toString();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#getValue(java.lang.String)
     */
    public Object getValue(String key)
    {
        return getProperty(key);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.PersistableStorage#init()
     */
    public void init()
        throws Exception
    {
        if (_storageProvider == null)
            initProvider();
        
        if (_storageProvider instanceof PersistableStorage)
            ((PersistableStorage)_storageProvider).init();
        else
            new IllegalStateException(
                    "init() is not supported by storage provider");
    }
    
    /**
     * Initializes the StorageProvider.
     */
    private void initProvider()
    {
        try
        {
            _storageProvider = (StorageProvider)ObjectFactory.instance().get(
                    StorageProvider.class.getName(),
                    LocalStorageProvider.class.getName(), false);
        }
        catch (RuntimeException ex)
        {
            _storageProvider = new LocalStorageProvider();
            
            Logger.log(Logger.SEVERE, getClass(),
                    StorageResource.CANNOT_INIT_NO_PROVIDER.getValue());
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.PersistableStorage#load()
     */
    public void load()
        throws Exception
    {
        if (_storageProvider == null)
            initProvider();
        
        if (_storageProvider instanceof PersistableStorage)
            ((PersistableStorage)_storageProvider).load();
        else
            new IllegalStateException(
                    "load() is not supported by storage provider");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.PersistableStorage#load(java.util.List)
     */
    public void load(List<String> keys)
        throws Exception
    {
        if (_storageProvider == null)
            initProvider();
        
        if (_storageProvider instanceof PersistableStorage)
            ((PersistableStorage)_storageProvider).load(keys);
        else
            new IllegalStateException(
                    "load(...) is not supported by storage provider");
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.storage.StorageProvider#removeProperty(java.lang.String)
     */
    public Object removeProperty(String key)
    {
        if (_storageProvider == null)
            initProvider();
        
        return _storageProvider.removeProperty(key);
    }
    
    /**
     * Sets the properties by parsing string like key;value|key;value
     * 
     * @param text
     *            the new properties
     */
    public void setProperties(String text)
    {
        StorageUtils.setProperties(_storageProvider, text);
    }
    
    /**
     * Sets the property.
     * 
     * @param key
     *            the key
     * @param value
     *            the value
     * @return the object
     */
    public Object setProperty(String key, Object value)
    {
        if (_storageProvider == null)
            initProvider();
        
        return _storageProvider.setProperty(key, new StorageObject(value));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.StorageProvider#setProperty(java.lang.String,
     * com.toolsverse.storage.StorageProvider.StorageObject)
     */
    public Object setProperty(String key, StorageObject value)
    {
        if (_storageProvider == null)
            initProvider();
        
        return _storageProvider.setProperty(key, value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#setValue(java.lang.String,
     * java.lang.Object)
     */
    public void setValue(String key, Object value)
    {
        setProperty(key, value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.PersistableStorage#store()
     */
    public void store()
        throws Exception
    {
        if (_storageProvider == null)
            initProvider();
        
        if (_storageProvider instanceof PersistableStorage)
            ((PersistableStorage)_storageProvider).store();
        else
            new IllegalStateException(
                    "store() is not supported by storage provider");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.PersistableStorage#store(java.util.Map)
     */
    public void store(Map<String, StorageObject> properties)
        throws Exception
    {
        if (_storageProvider == null)
            initProvider();
        
        if (_storageProvider instanceof PersistableStorage)
            ((PersistableStorage)_storageProvider).store(properties);
        else
            new IllegalStateException(
                    "store(...) is not supported by storage provider");
    }
    
}