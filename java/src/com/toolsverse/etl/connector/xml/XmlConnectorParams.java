/*
 * XmlConnectorParams.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.xml;

import java.util.Map;
import java.util.Properties;

import org.w3c.dom.Node;

import com.toolsverse.cache.CacheProvider;
import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.connector.DataSetConnectorParams;
import com.toolsverse.etl.connector.FileConnectorParams;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.Utils;
import com.toolsverse.util.XmlUtils;

/**
 * The {@link DataSetConnectorParams} used by XmlConnector.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class XmlConnectorParams extends FileConnectorParams
{
    
    /** The PREFIX. */
    public static final String PREFIX = "xml";
    
    /** The XSL FILE NAME property. */
    public static final String XSL_FILE_NAME_PROP = "xsl";
    
    /** The XSL "FROM" FILE NAME property. */
    public static final String XSL_FROM_FILE_NAME_PROP = "xslfrom";
    
    /** The XSL "TO" FILE NAME property. */
    public static final String XSL_TO_FILE_NAME_PROP = "xslto";
    
    /** The DOM xml model. */
    private XmlUtils _xml;
    
    /** The current DOM node. */
    private Node _currentNode;
    
    /** The xsl file name. */
    private String _xslFileName;
    
    /** The xsl "from" file name. */
    private String _xslFromFileName;
    
    /** The xsl "to" file name. */
    private String _xslToFileName;
    
    /** The temporary file name used during xsl transformation. */
    private String _tempFileName;
    
    /**
     * Instantiates a new XmlConnectorParams.
     */
    public XmlConnectorParams()
    {
        this(null, true, -1);
    }
    
    /**
     * Instantiates a new XmlConnectorParams.
     *
     * @param cacheProvider the cache provider
     * @param silent the "is silent" flag
     * @param logStep the log step
     */
    public XmlConnectorParams(CacheProvider<String, Object> cacheProvider,
            boolean silent, int logStep)
    {
        super(cacheProvider, silent, logStep);
        
        _xml = null;
        _xslFileName = null;
        _xslFromFileName = null;
        _xslToFileName = null;
        _currentNode = null;
        _tempFileName = null;
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.DataSetConnectorParams#copy()
     */
    @Override
    public DataSetConnectorParams copy()
    {
        XmlConnectorParams params = new XmlConnectorParams();
        
        params.setFileNameRequired(isFileNameRequired());
        params.setFileName(getFileName());
        
        params.setXslFileName(_xslFileName);
        params.setXslFromFileName(_xslFromFileName);
        params.setXslToFileName(_xslToFileName);
        
        params.setDateFormat(getDateFormat());
        params.setDateTimeFormat(getDateTimeFormat());
        params.setTimeFormat(getTimeFormat());
        params.setUseSelectedDataSet(useSelectedDataSet());
        params.setSplitBy(getSplitBy());
        
        return params;
    }
    
    /**
     * Gets the current DOM node.
     *
     * @return the current DOM node
     */
    public Node getCurrentNode()
    {
        return _currentNode;
    }
    
    /**
     * Gets the temporary file name.
     *
     * @return the temporary file name
     */
    public String getTempFileName()
    {
        return _tempFileName;
    }
    
    /**
     * Gets the xml as DOM model.
     *
     * @return the xml
     */
    public XmlUtils getXml()
    {
        return _xml;
    }
    
    /**
     * Gets the xsl file name.
     *
     * @param value the value
     * @param fileName the file name
     * @return the xsl file name
     */
    private String getXslFileName(String value, String fileName)
    {
        if (!Utils.isNothing(value))
        {
            String folder = Utils.isNothing(FilenameUtils.getFullPath(value))
                    && !Utils.isNothing(FilenameUtils.getFullPath(fileName)) ? FilenameUtils
                    .getFullPath(fileName) : getFolder();
            
            return FileUtils.getFilename(value, folder, "*.xsl", false);
        }
        else
            return null;
    }
    
    /**
     * Gets the xsl "from" file name.
     *
     * @param fileName the file name
     * @return the xsl "from" file name
     */
    public String getXslFromFileName(String fileName)
    {
        if (!Utils.isNothing(_xslFromFileName))
            return getXslFileName(_xslFromFileName, fileName);
        else
            return getXslFileName(_xslFileName, fileName);
    }
    
    /**
     * Gets the xsl "to" file name.
     *
     * @param fileName the file name
     * @return the xsl "to" file name
     */
    public String getXslToFileName(String fileName)
    {
        if (!Utils.isNothing(_xslToFileName))
            return getXslFileName(_xslToFileName, fileName);
        else
            return getXslFileName(_xslFileName, fileName);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.FileConnectorParams#init(com.toolsverse.
     * etl.common.Alias)
     */
    @Override
    public void init(Alias alis)
    {
        super.init(alis);
        
        Properties props = Utils.getProperties(alis.getParams());
        
        if (props == null || props.size() == 0)
            return;
        
        setXslFileName(SystemConfig.instance().getPathUsingAppFolders(
                props.getProperty(XSL_FILE_NAME_PROP)));
        setXslFromFileName(SystemConfig.instance().getPathUsingAppFolders(
                props.getProperty(XSL_FROM_FILE_NAME_PROP)));
        setXslToFileName(SystemConfig.instance().getPathUsingAppFolders(
                props.getProperty(XSL_TO_FILE_NAME_PROP)));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.FileConnectorParams#init(java.util.Map)
     */
    @Override
    public void init(Map<String, String> props)
    {
        super.init(props);
        
        if (props == null)
            return;
        
        setXslFileName(SystemConfig.instance().getPathUsingAppFolders(
                props.get(XSL_FILE_NAME_PROP)));
        setXslFromFileName(SystemConfig.instance().getPathUsingAppFolders(
                props.get(XSL_FROM_FILE_NAME_PROP)));
        setXslToFileName(SystemConfig.instance().getPathUsingAppFolders(
                props.get(XSL_TO_FILE_NAME_PROP)));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnectorParams#init(com.toolsverse
     * .util.ObjectStorage)
     */
    @Override
    public void init(ObjectStorage storage)
    {
        if (storage == null)
            return;
        
        setFileNameRequired(true);
        
        setFileName(SystemConfig.instance().getPathUsingAppFolders(
                storage.getString(PREFIX + FILE_NAME_PROP)));
        
        setDateFormat(storage.getString(PREFIX + SqlUtils.DATE_FORMAT_PROP));
        setTimeFormat(storage.getString(PREFIX + SqlUtils.TIME_FORMAT_PROP));
        setDateTimeFormat(storage.getString(PREFIX
                + SqlUtils.DATE_TIME_FORMAT_PROP));
        
        setXslFileName(SystemConfig.instance().getPathUsingAppFolders(
                storage.getString(PREFIX + XSL_FILE_NAME_PROP)));
        setXslFromFileName(SystemConfig.instance().getPathUsingAppFolders(
                storage.getString(PREFIX + XSL_FROM_FILE_NAME_PROP)));
        setXslToFileName(SystemConfig.instance().getPathUsingAppFolders(
                storage.getString(PREFIX + XSL_TO_FILE_NAME_PROP)));
        
        setUseSelectedDataSet(Utils.str2Boolean(
                storage.getString(PREFIX + USE_SELECTED_PROP), false));
        
        setSplitBy(storage.getString(PREFIX + SPLIT_BY_PROP));
    }
    
    /**
     * Sets the current DOM node.
     *
     * @param value the new current DOM node
     */
    public void setCurrentNode(Node value)
    {
        _currentNode = value;
    }
    
    /**
     * Sets the temporary file name.
     *
     * @param value the new temporary file name
     */
    public void setTempFileName(String value)
    {
        _tempFileName = value;
    }
    
    /**
     * Sets the xml.
     *
     * @param value the new xml
     */
    public void setXml(XmlUtils value)
    {
        _xml = value;
    }
    
    /**
     * Sets the xsl file name.
     *
     * @param value the new xsl file name
     */
    public void setXslFileName(String value)
    {
        _xslFileName = value;
    }
    
    /**
     * Sets the xsl "from" file name.
     *
     * @param value the new xsl "from" file name
     */
    public void setXslFromFileName(String value)
    {
        _xslFromFileName = value;
    }
    
    /**
     * Sets the xsl "to" file name.
     *
     * @param value the new xsl "to" file name
     */
    public void setXslToFileName(String value)
    {
        _xslToFileName = value;
    }
}
