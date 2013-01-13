/*
 * SqlConfig.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.config;

import java.io.File;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.license.ClientCertificate;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.Utils;
import com.toolsverse.util.XmlUtils;
import com.toolsverse.util.encryption.SymmetricEncryptor;
import com.toolsverse.util.log.Logger;

/**
 * The singleton class which holds a reference to the map of the named aliases. The aliases stored in app_root\config\connections\aliases.xml file and can be used
 * to create database connections. The alias with the name "system" points to the system database which contains nodes, roles, rights, users 
 * and other system artifacts. 
 * 
 * @see com.toolsverse.etl.common.Alias
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SqlConfig
{
    
    /** The default sql configuration file name. */
    public static String DEFAULT_SQL_CONFIG = "aliases.xml";
    
    /** The folder for the sql configuration file. */
    private static final String CONNECTIONS_FOLDER = "connections";
    
    // xml dictionary
    
    /** The CONFIG. */
    public static final String CONFIG = "config";
    
    /** The CONNECTIONS. */
    public static final String CONNECTIONS = "connections";
    
    /** The CONNECTION. */
    public static final String CONNECTION = "connection";
    
    /** The CONNECTION_ALIAS. */
    public static final String CONNECTION_ALIAS = "alias";
    
    /** The DRIVER. */
    public static final String DRIVER = "driver";
    
    /** The URL. */
    public static final String URL = "url";
    
    /** The USER_ID. */
    public static final String USER_ID = "userid";
    
    /** The PASSWORD. */
    public static final String PASSWORD = "password";
    
    /** The ALIAS_PARAMS. */
    public static final String ALIAS_PARAMS = "params";
    
    /** The INIT_SQL. */
    public static final String INIT_SQL = "sql";
    
    /** The CONNECOR_CLASS_NAME. */
    public static final String CONNECOR_CLASS_NAME = "connector";
    
    /**
     * Gets the singleton instance of the SqlConfig.
     *
     * @return the SqlConfig
     * @throws Exception in case of any error
     */
    public static SqlConfig instance()
        throws Exception
    {
        if (_instance == null)
        {
            synchronized (SqlConfig.class)
            {
                if (_instance == null)
                    _instance = new SqlConfig();
            }
        }
        
        return _instance;
    }
    
    /** The aliases. */
    private Map<String, Alias> _aliases;
    
    // volatile is needed so that multiple thread can reconcile the instance
    /** The instance of the SqlConfig. */
    private volatile static SqlConfig _instance;
    
    /**
     * Instantiates a new SqlConfig.
     *
     * @throws Exception in case of any error
     */
    private SqlConfig() throws Exception
    {
        _aliases = null;
        
        init();
    }
    
    /**
     * Adds the alias.
     * 
     * @param alias
     *            The alias
     * 
     * @see com.toolsverse.etl.common.Alias
     */
    private void addAlias(Alias alias)
    {
        if (_aliases == null)
            _aliases = new ConcurrentHashMap<String, Alias>();
        
        _aliases.put(alias.getName(), alias);
    }
    
    /**
     * Gets the alias by name.
     *
     * @param name the name
     * @return the alias
     */
    public Alias getAlias(String name)
    {
        return _aliases != null ? _aliases.get(name) : null;
    }
    
    /**
     * Gets the folder for the sql configuration file.
     *
     * @return the folder
     */
    private String getFolder()
    {
        return SystemConfig.instance().getConfigFolderName()
                + CONNECTIONS_FOLDER;
    }
    
    /**
     * Reads the sql configuration file.
     *
     * @throws Exception in case of any error
     */
    public void init()
        throws Exception
    {
        InputStream dataStream = null;
        File file = null;
        XmlUtils xml = null;
        
        String configFileName = getFolder() + "/" + DEFAULT_SQL_CONFIG;
        
        if (!Utils.isNothing(configFileName))
        {
            file = new File(configFileName);
            if (file != null && file.exists())
                xml = new XmlUtils(file);
            else
            {
                dataStream = SqlConfig.class.getClassLoader()
                        .getResourceAsStream(
                                SystemConfig.instance().getConfigFolderName()
                                        + configFileName);
                if (dataStream != null)
                    xml = new XmlUtils(dataStream);
            }
            
            if (xml != null)
                loadConfigXml(xml);
            else
                Logger.log(Logger.WARNING, EtlLogger.class,
                        EtlResource.CONFIG_FILE_IS_MISSING_MSG.getValue());
        }
        else
            Logger.log(Logger.WARNING, EtlLogger.class,
                    EtlResource.CONFIG_FILE_IS_MISSING_MSG.getValue());
    }
    
    /**
     * Loads aliases from the dom xml model.
     *
     * @param xml the dom xml model
     * @throws Exception in case of any error
     */
    private void loadConfigXml(XmlUtils xml)
        throws Exception
    {
        Node rootNode;
        Node connNode;
        Node cNode;
        NodeList nodeList;
        Alias alias;
        
        String value;
        
        // config
        rootNode = xml.getFirstNodeNamed(CONFIG);
        if (rootNode == null)
        {
            Logger.log(Logger.WARNING, EtlLogger.class,
                    EtlResource.CONFIG_IS_MISSING_MSG.getValue());
            
            return;
        }
        
        // connections
        connNode = xml.getFirstNodeNamed(rootNode, CONNECTIONS);
        if (connNode == null)
        {
            Logger.log(Logger.WARNING, EtlLogger.class,
                    EtlResource.CONNECTIONS_IS_MISSING_MSG.getValue());
            
            return;
        }
        
        // aliases
        nodeList = connNode.getChildNodes();
        
        if ((nodeList == null) || (nodeList.getLength() == 0))
        {
            Logger.log(Logger.WARNING, EtlLogger.class,
                    EtlResource.CONNECTIONS_ARE_MISSING_MSG.getValue());
            
            return;
        }
        
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            cNode = nodeList.item(i);
            
            if ((cNode.getNodeType() == Node.TEXT_NODE)
                    || (cNode.getNodeType() == Node.COMMENT_NODE))
                continue;
            
            alias = new Alias();
            
            // name
            value = xml.getStringAttribute(cNode, CONNECTION_ALIAS);
            if (Utils.isNothing(value))
                continue;
            alias.setName(value);
            
            // driver
            alias.setJdbcDriverClass(xml.getNodeValue(cNode, DRIVER));
            
            // url
            alias.setUrl(xml.getNodeValue(cNode, URL));
            
            // user id
            alias.setUserId(xml.getNodeValue(cNode, USER_ID));
            
            // password
            String password = xml.getNodeValue(cNode, PASSWORD);
            if (!Utils.isNothing(password))
                try
                {
                    password = SymmetricEncryptor.decryptPassword(
                            ClientCertificate.instance().getKey(), password);
                }
                catch (GeneralSecurityException ex)
                {
                    Logger.log(Logger.SEVERE, this,
                            Resource.ERROR_DECRYPTING.getValue(), ex);
                }
            
            alias.setPassword(password);
            
            // params
            alias.setParams(xml.getNodeValue(cNode, ALIAS_PARAMS));
            
            // start sql
            alias.setInitSql(xml.getNodeValue(cNode, INIT_SQL));
            
            // start sql
            alias.setConnectorClassName(xml.getNodeValue(cNode,
                    CONNECOR_CLASS_NAME));
            
            addAlias(alias);
        }
    }
    
}
