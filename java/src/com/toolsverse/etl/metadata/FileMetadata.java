/*
 * FileMetadata.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.KeyValue;
import com.toolsverse.util.Utils;

/**
 * The abstract implementation of Metadata interface for the file based data
 * sources such as text file, xml file, Excel, etc.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class FileMetadata extends BaseMetadata
{
    
    /** The type methods. */
    private static Map<String, String> TYPE_METHODS = new HashMap<String, String>();
    
    /** The metadata methods. */
    private static Map<String, String> METADATA_METHODS = new HashMap<String, String>();
    
    /** The types by parent. */
    private static Map<String, String> TYPES_BY_PARENT = new HashMap<String, String>();
    static
    {
        TYPE_METHODS.put(TYPE_CATALOG, "getTableTypes");
    }
    
    static
    {
        METADATA_METHODS.put(TYPE_TABLES, "getTablesByType");
    }
    static
    {
        TYPES_BY_PARENT.put(TYPE_TABLES, TYPE_TABLE);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#discoverDatabaseTypes()
     */
    public Map<Integer, List<FieldDef>> discoverDatabaseTypes()
        throws Exception
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#free()
     */
    public void free()
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.BaseMetadata#getMetadataMethods()
     */
    @Override
    public Map<String, String> getMetadataMethods()
    {
        return METADATA_METHODS;
    }
    
    /**
     * Gets the tables by type.
     * 
     * @param inputSteam
     *            the input steam
     * @param name
     *            the name
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the tables by type
     * @throws Exception
     *             in case of any error
     */
    public abstract DataSet getTablesByType(InputStream inputSteam,
            String name, String pattern, String type)
        throws Exception;
    
    /**
     * Gets the tables by type.
     * 
     * @param catalog
     *            the catalog
     * @param schema
     *            the schema
     * @param pattern
     *            the pattern
     * @param type
     *            the type
     * @return the tables by type
     * @throws Exception
     *             in case of any error
     */
    public abstract DataSet getTablesByType(String catalog, String schema,
            String pattern, String type)
        throws Exception;
    
    /**
     * Gets the table types.
     * 
     * @return the table types
     * @throws Exception
     *             in case of any error
     */
    public List<String> getTableTypes()
        throws Exception
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_TABLES);
        
        return list;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#getTopLevelDbObjects()
     */
    public List<Object> getTopLevelDbObjects()
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        String path = getConnectionParamsProvider().getConnectionParams()
                .getUrl();
        
        if (Utils.isNothing(path))
            return null;
        
        String folder = FilenameUtils.getFullPath(path);
        
        if (Utils.isNothing(folder))
            folder = SystemConfig.instance().getDataFolderName();
        
        String fName = FilenameUtils.getName(path);
        
        if (Utils.isNothing(fName))
            return null;
        
        File[] files = FileUtils.getFilesInFolder(folder, fName);
        
        if (files == null || files.length == 0)
            return null;
        
        setHasCatalogs(true);
        setHasSchemas(false);
        
        List<Object> list = new ArrayList<Object>();
        
        for (File file : files)
        {
            list.add(new KeyValue(file.getAbsolutePath(), null));
        }
        
        setCurrentDatabase(files[0].getAbsolutePath());
        
        return list;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#getTypeMethods()
     */
    public Map<String, String> getTypeMethods()
    {
        return TYPE_METHODS;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.BaseMetadata#getTypesByParent()
     */
    @Override
    public Map<String, String> getTypesByParent()
    {
        return TYPES_BY_PARENT;
    }
    
}