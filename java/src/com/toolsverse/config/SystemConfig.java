/*
 * SystemConfig.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.config;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.toolsverse.exception.DefaultExceptionHandler;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.UrlUtils;
import com.toolsverse.util.Utils;
import com.toolsverse.util.collector.AppInfo;
import com.toolsverse.util.collector.AppInfoCollector;
import com.toolsverse.util.collector.AppInfoCollectorDefault;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.factory.ObjectFactoryModule;
import com.toolsverse.util.log.Logger;

/**
 * The main configuration class for the Toolsverse framework. All components have a dependency on SystemConfig. This is a singleton class which when instantiated
 * loads properties from the various property files in the file system and from the classpath, registers itself as a ObjectFactoryModule and sets path 
 * to the HOME, CONFIG, DATA and other default app folders.
 * 
 * The basic idea is to have a hierarchy of the configuration files. The file in the top of the food chain is a config.property in the CONFIG folder. 
 * It overrides everything. The rest of the configuration files are usually reside inside packages and deployed with the code in jar files. These files
 * contain properties for the specific core or extension modules and are independent from each other.  
 *
 * @see com.toolsverse.util.factory.ObjectFactoryModule
 * @see com.toolsverse.util.collector.AppInfoCollector
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SystemConfig implements ObjectFactoryModule, Serializable
{
    
    /** DEFAULT VENDOR. */
    public static final String DEFAULT_VENDOR = "Toolsverse";
    
    /** The CLIENT MODE name. */
    public static final String CLIENT_MODE = "Client";
    
    /** The CLIENT SERVER MODE name. */
    public static final String CLIENT_SERVER_MODE = "Client-Server";
    
    /** The WEB MODE name. */
    public static final String WEB_MODE = "Web";
    
    /** CLIENT DEPLOYMENT - regular client app without server tier. */
    public static final String CLIENT_DEPLOYMENT = "client";
    
    /** SERVER DEPLOYMENT - server side deployment. */
    public static final String SERVER_DEPLOYMENT = "server";
    
    /** TEST_DEPLOYMENT - unit test. */
    public static final String TEST_DEPLOYMENT = "test";
    
    // path
    
    /** WORKING PATH. */
    public static final String WORKING_PATH = System.getProperty("user.dir");
    
    /** UPDATE PATH suffix. */
    private static final String UPDATE_FOLDER_PATH = "/update/";
    
    /** CONFIG FOLDER PATH suffix. */
    private static final String CONFIG_FOLDER_PATH = "/config/";
    
    /** DATA FOLDER PATH suffix. */
    public static final String DATA_FOLDER_PATH = "/data/";
    
    /** Location of the main properties file. */
    private static final String CONFIG_RESOURCE_PATH = "com/toolsverse/config/";
    
    /** The property which defines the name of the main properties file. */
    private static final String CONFIG_FILE_NAME = "config.file.name";
    
    /** The name of the main properties file. */
    private static final String PROPS_FILE_NAME = "config.properties";
    
    /** The default prefix for the framework jar files. */
    public static final String JAR_PREFIX = "toolsverse";
    
    /** LIB PATH suffix. */
    public static final String LIB_PATH = "/lib/";
    
    /** PLUGIN PATH suffix. */
    public static final String PLUGIN_PATH = "/plugin/";
    
    /** ERRORS PATH suffix. */
    public static final String ERRORS_PATH = "errors/";
    
    /** SCRIPTS PATH suffix. */
    public static final String SCRIPTS_PATH = "scripts/";
    
    /** JDBC PATH suffix. */
    public static final String JDBC_PATH = "jdbc/";
    
    // props
    
    /** SERVER URL property. Must be set on the client. If it is configured app works in the client-server mode*/
    public static final String SERVER_URL = "app.server.url";
    
    /** UPDATE URL property. Must be set on the client. If it is configured app can request and download updates */
    public static final String UPDATE_URL = "app.update.url";
    
    /** SERVER PROXY HOST property. */
    public static final String SERVER_PROXY_HOST = "app.server.proxy.host";
    
    /** SERVER PROXY PORT property. */
    public static final String SERVER_PROXY_PORT = "app.server.proxy.port";
    
    /** HOME PATH PROPERTY. If property is set it overrides default HOME location. */
    public static final String HOME_PATH_PROPERTY = "app.home";
    
    /** DATA PATH PROPERTY. If property is set it overrides default DATA location. */
    public static final String DATA_PATH_PROPERTY = "app.data";
    
    /** The ROOT DATA PATH PROPERTY. */
    public static final String ROOT_DATA_PATH_PROPERTY = "app.root.data";
    
    /** DEPLOYMENT PROPERTY. Possible values: client,server,test */
    public static final String DEPLOYMENT_PROPERTY = "app.deployment";
    
    /** SHELL EXTENSION PROP. I property is set it overrides default shell script extension for the current OS */
    public static final String SHELL_EXT_PROP = "app.shellext";
    
    /** TITLE property. */
    public static final String TITLE = "app.title";
    
    /** VENDOR property. */
    public static final String VENDOR = "app.vendor";
    
    /** COPYRIGHT property. */
    public static final String COPYRIGHT = "app.copyright";
    
    /** VERSION property. */
    public static final String VERSION = "app.version";
    
    /** The APPLICATION NAME property. */
    public static final String APP_NAME = "app.name";
    
    /** The APPLICATION FRAME ICON property. */
    public static final String APP_FRAME_ICON = "app.frame.icon";
    
    /** SHARED CLASSPATH property. Used by extensions framework to load modules shared between all sessions. */
    public static final String SHARED_CLASSPATH = "shared.classpath";
    
    /** LOCAL CLASSPATH property. Used by extensions framework to load modules specific to the current session.*/
    public static final String LOCAL_CLASSPATH = "local.classpath";
    
    // volatile is needed so that multiple thread can reconcile the instance
    
    /** The instance of the SystemConfig. */
    private volatile static SystemConfig _instance;
    
    /**
     * Returns SystemConfig instance. Instantiates it if needed. 
     *
     * @return the system config
     */
    public static SystemConfig instance()
    {
        if (_instance == null)
        {
            synchronized (SystemConfig.class)
            {
                if (_instance == null)
                    _instance = new SystemConfig();
            }
        }
        
        return _instance;
    }
    
    /** The _home. */
    private String _home;
    
    /** The _libs path. */
    private String _libsPath;
    
    /** The _plugins path. */
    private String _pluginsPath;
    
    /** The _settings folder name. */
    private String _settingsFolderName;
    
    /** The _data folder name. */
    private String _dataFolderName;
    
    /** The _sys properties. */
    private Map<String, String> _sysProperties;
    
    /** The _app info collector. */
    private AppInfoCollector _appInfoCollector;
    
    /**
     * Instantiates a new SystemConfig.
     */
    private SystemConfig()
    {
        _home = null;
        _libsPath = null;
        _pluginsPath = null;
        _settingsFolderName = null;
        _dataFolderName = null;
        
        init();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.util.factory.ObjectFactoryModule#bind(java.lang.String,
     * java.lang.String)
     */
    public void bind(String fromName, String toName)
    {
        setSystemProperty(fromName, toName);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.util.factory.ObjectFactoryModule#get(java.lang.String)
     */
    public String get(String name)
    {
        String value = null;
        
        String deployment = getSystemProperty(DEPLOYMENT_PROPERTY);
        
        if (!Utils.isNothing(deployment))
        {
            value = getSystemProperty(name + "." + deployment);
        }
        
        if (Utils.isNothing(value))
        {
            value = getSystemProperty(name);
        }
        
        return value;
    }
    
    /**
     * Gets the application root path.
     *
     * @return the application root path
     */
    public AppInfo getAppInfo()
    {
        return getAppInfoCollector().getAppInfo();
    }
    
    /**
     * Gets the AppInfoCollector specific to the deployment mode. 
     *
     * @return the AppInfoCollector
     */
    private AppInfoCollector getAppInfoCollector()
    {
        if (_appInfoCollector == null)
            _appInfoCollector = (AppInfoCollector)ObjectFactory.instance().get(
                    AppInfoCollector.class.getName(),
                    AppInfoCollectorDefault.class.getName(), true);
        
        return _appInfoCollector;
    }
    
    /**
     * Gets the application name.
     *
     * @return the application name
     */
    public String getAppName()
    {
        return getSystemProperty(APP_NAME, DEFAULT_VENDOR);
    }
    
    /**
     * Gets the configuration folder path.
     *
     * @return the configuration folder path
     */
    public String getConfigFolderName()
    {
        return _settingsFolderName;
    }
    
    /**
     * Gets the data folder path. If application works in client-server or web mode the data folder path is calculated using current user id.
     *
     * @return the data folder path
     */
    public String getDataFolderName()
    {
        String folder = _dataFolderName;
        
        try
        {
            if (isClient())
                folder = _dataFolderName;
            else
            {
                folder = FilenameUtils.separatorsToUnix(FilenameUtils
                        .normalize(_dataFolderName
                                + getAppInfoCollector().getAppInfo()
                                        .getUniqueName()));
            }
        }
        finally
        {
            File file = new File(folder);
            
            if (!file.exists())
                file.mkdirs();
        }
        
        return folder;
    }
    
    /**
     * Gets the deployment type. Possible values: client, server, test
     *
     * @return the deployment type
     */
    public String getDeploymentType()
    {
        return getSystemProperty(DEPLOYMENT_PROPERTY, CLIENT_DEPLOYMENT);
    }
    
    /**
     * Gets the errors folder path. If application works in client-server or web mode the error folder path is calculated using current user id.
     *
     * @return the errors folder
     */
    public String getErrorsFolder()
    {
        String folder = FilenameUtils.separatorsToUnix(FilenameUtils
                .normalize(getDataFolderName() + "/" + ERRORS_PATH));
        
        File file = new File(folder);
        
        if (!file.exists())
            file.mkdirs();
        
        return folder;
    }
    
    /**
     * Gets the home folder path.
     *
     * @return the home folder path
     */
    public String getHome()
    {
        return _home;
    }
    
    /**
     * Gets the libs path. 
     *
     * @return the libs path
     */
    public String getLibsPath()
    {
        return _libsPath;
    }
    
    /**
     * Gets the mode.
     *
     * @return the mode
     */
    public String getMode()
    {
        if (isServer())
            return WEB_MODE;
        else if (isClientServer())
            return CLIENT_SERVER_MODE;
        else
            return CLIENT_MODE;
    }
    
    /**
     * Returns system property using the following algorithm: <code>getSystemProperty(object.getClass().getName() + "." + object.toString(),
     * object.toString())</code>
     *
     * @param object the object
     * @return system property
     */
    public String getObjectProperty(Object object)
    {
        if (object == null)
            return null;
        
        String value = object.toString();
        
        if (value == null)
            return null;
        
        return getSystemProperty(object.getClass().getName() + "." + value,
                value);
    }
    
    /**
     * Gets the path by property. If path is not found in app properties the defaultPath is used. 
     *
     * @param propName the property name
     * @param defaultPath the default path
     * @return the path by property
     */
    public String getPathByProp(String propName, String defaultPath)
    {
        String path = getSystemProperty(propName);
        if (Utils.isNothing(path))
            path = defaultPath;
        
        return path;
    }
    
    /**
     * Gets the path using default app folders {app.home}, {app.data} and {app.rootdata}.
     * 
     * <br>
     * Example:
     * <br>
     * {app.home}\somepath\path --> c:\data explorer\somepath\path
     * <br>
     * {app.data}\somepath\path --> c:\data explorer\data\somepath\path
     * <br>
     * {app.root.data}\demo --> c:\data explorer\data\demo
     *
     * @param path the path
     * @return the path 
     */
    public String getPathUsingAppFolders(String path)
    {
        if (path == null)
            return path;
        
        return Utils.findAndReplaceRegexp(Utils.findAndReplaceRegexp(Utils
                .findAndReplaceRegexp(path, "{" + HOME_PATH_PROPERTY + "}",
                        getHome(), true), "{" + DATA_PATH_PROPERTY + "}",
                getDataFolderName(), true),
                "{" + ROOT_DATA_PATH_PROPERTY + "}", getRootDataFolderPath(),
                true);
        
    }
    
    /**
     * Gets the plug ins path.
     *
     * @return the plug ins path
     */
    public String getPluginsPath()
    {
        return _pluginsPath;
    }
    
    /**
     * Gets the properties by mask.
     *
     * @param mask the mask
     * 
     * @return the properties by mask
     */
    public Map<String, String> getPropsByMask(String mask)
    {
        Map<String, String> props = new HashMap<String, String>();
        
        if (_sysProperties == null)
            return props;
        
        for (String key : _sysProperties.keySet())
        {
            if (key.startsWith(mask))
                props.put(key, _sysProperties.get(key));
        }
        
        return props;
    }
    
    /**
     * Gets the root data folder path.
     *
     * @return the root data folder path
     */
    public String getRootDataFolderPath()
    {
        return _dataFolderName;
    }
    
    /**
     * Gets the scripts folder path.
     *
     * @return the scripts folder path
     */
    public String getScriptsFolder()
    {
        String folder = FilenameUtils.separatorsToUnix(FilenameUtils
                .normalize(getDataFolderName() + "/" + SCRIPTS_PATH));
        
        File file = new File(folder);
        
        if (!file.exists())
            file.mkdirs();
        
        return folder;
    }
    
    /**
     * Gets the server url.
     *
     * @return the server url
     */
    public String getServerUrl()
    {
        return getSystemProperty(SERVER_URL, "");
    }
    
    /**
     * Gets the system properties.
     *
     * @return the system properties
     */
    public Map<String, String> getSystemProperties()
    {
        return _sysProperties;
    }
    
    /**
     * Gets the system property by name.
     *
     * @param name the name of the property
     * @return the system property
     */
    public String getSystemProperty(String name)
    {
        return getSystemProperty(name, null);
    }
    
    /**
     * Gets the system property by name. If property is not found uses defaultValue.
     *
     * @param name the name
     * @param defaultValue the default value
     * @return the system property
     */
    public String getSystemProperty(String name, String defaultValue)
    {
        if (_sysProperties == null || !_sysProperties.containsKey(name))
            return defaultValue;
        
        String value = _sysProperties.get(name).trim();
        
        if (value == null || value.indexOf("@") != 0)
            return value;
        
        value = value.substring(1);
        
        String[] params = Utils.getFunctionParams("decode", value);
        
        if (params == null || params.length == 0)
            return value;
        
        for (int i = 0; i < params.length; i++)
        {
            String param = params[i].trim();
            
            String paramValue = null;
            
            if (param.indexOf("@") == 0)
            {
                paramValue = param.substring(1);
                
                if ("null".equalsIgnoreCase(paramValue))
                    paramValue = null;
            }
            else
                paramValue = _sysProperties.get(param);
            
            params[i] = paramValue != null ? paramValue.trim() : null;
        }
        
        String ret = (String)Utils.decodeArray(params);
        
        return ret != null ? ret : defaultValue;
    }
    
    /**
     * Gets the system property for deployment. The same property can have different values for different deployments.
     * 
     * <p>
     * Example:
     * <p>
     * app.test.client=abc
     * <p>
     * app.test.server=xyz
     *
     * @param name the name
     * @param defaultValue the default value
     * @return the system property for deployment
     */
    public String getSystemPropertyForDeployment(String name,
            String defaultValue)
    {
        String deployment = SystemConfig.instance().getDeploymentType();
        
        if (_sysProperties != null
                && _sysProperties.containsKey(name + "." + deployment))
            return getSystemProperty(name + "." + deployment, defaultValue);
        else
            return getSystemProperty(name, defaultValue);
    }
    
    /**
     * Gets the app title.
     *
     * @param defaultTile the default tile
     * @return the title
     */
    public String getTitle(String defaultTile)
    {
        return getSystemProperty(VENDOR, DEFAULT_VENDOR) + " "
                + getSystemProperty(TITLE, Utils.makeString(defaultTile));
    }
    
    /**
     * Gets the update folder.
     *
     * @return the update folder
     */
    public String getUpdateFolder()
    {
        return getHome() + UPDATE_FOLDER_PATH;
    }
    
    /**
     * Gets the product version.
     *
     * @return the version
     */
    public String getVersion()
    {
        return getSystemProperty(VERSION, "3.1");
    }
    
    /**
     * Reads all properties, configures system folders, etc.
     */
    private void init()
    {
        ObjectFactory.instance().register(this);
        
        loadSystemProps();
        
        setPath();
        
        loadOtherProps();
    }
    
    /**
     * Checks if it is a client deployment.
     *
     * @return true, if it is a client deployment
     */
    public boolean isClient()
    {
        return !SERVER_DEPLOYMENT.equalsIgnoreCase(getDeploymentType());
    }
    
    /**
     * Checks if it is a client-server mode. The client-server mode is set when it is a client deployment and server URL is defined. 
     *
     * @return true, if it is a client-server mode
     */
    
    public boolean isClientServer()
    {
        return isClient() && !Utils.isNothing(getServerUrl());
    }
    
    /**
     * Checks if it is a server deployment.
     *
     * @return true, if it is a server deployment
     */
    public boolean isServer()
    {
        return SERVER_DEPLOYMENT.equalsIgnoreCase(getDeploymentType());
    }
    
    /**
     * Loads properties from the config.properties file in the CONFIG folder. Adds them to the system properties.
     */
    private void loadOtherProps()
    {
        
        String cfgFileName = System.getProperty(CONFIG_FILE_NAME,
                PROPS_FILE_NAME);
        
        Properties props = loadPropsFromFile(getConfigFolderName()
                + cfgFileName);
        
        if (props != null)
        {
            for (Object name : props.keySet())
            {
                _sysProperties.put(name.toString(), (String)props.get(name));
            }
        }
        
        if (!_sysProperties.containsKey(DEPLOYMENT_PROPERTY))
            _sysProperties.put(DEPLOYMENT_PROPERTY, CLIENT_DEPLOYMENT);
    }
    
    /**
     * Loads properties from the resourceName. Adds them to the system properties.  If property already exist - doesn't override it.
     *
     * @param resourceName the resource name
     */
    public void loadProps(String resourceName)
    {
        Properties props = loadPropsFromClassPath(resourceName);
        
        if (props != null)
        {
            for (Object name : props.keySet())
            {
                if (!_sysProperties.containsKey(name))
                    _sysProperties
                            .put(name.toString(), (String)props.get(name));
            }
        }
    }
    
    /**
     * Loads properties from class path.
     *
     * @param resourceName the resource name
     * @return the properties
     */
    public Properties loadPropsFromClassPath(String resourceName)
    {
        try
        {
            return Utils.loadPropsFromClassPath(resourceName);
        }
        catch (Exception ioe)
        {
            DefaultExceptionHandler.instance().logException(Logger.INFO,
                    getClass(), Resource.ERROR_LOADING_PROPS.getValue(), ioe);
            
            return null;
        }
    }
    
    /**
     * Loads properties from the file.
     *
     * @param fileName the file name
     * @return the properties
     */
    public Properties loadPropsFromFile(String fileName)
    {
        try
        {
            return Utils.loadPropsFromFile(fileName);
        }
        catch (IOException ioe)
        {
            DefaultExceptionHandler.instance().logException(Logger.SEVERE,
                    getClass(), Resource.ERROR_LOADING_PROPS.getValue(), ioe);
            
            return null;
        }
    }
    
    /**
     * Loads properties from the config.properties file in the CONFIG folder and jvm properties and adds them to the system properties.
     * 
     */
    private void loadSystemProps()
    {
        _sysProperties = new ConcurrentHashMap<String, String>();
        
        Map<Object, Object> newProps = new HashMap<Object, Object>();
        
        Properties props = loadPropsFromClassPath(CONFIG_RESOURCE_PATH
                + PROPS_FILE_NAME);
        
        if (props != null)
            newProps.putAll(props);
        
        newProps.putAll(System.getProperties());
        
        for (Object name : newProps.keySet())
        {
            Object value = newProps.get(name);
            
            if (value != null)
                _sysProperties.put(name.toString(), value.toString());
        }
    }
    
    /**
     * Calculates root code path. In web mode this is a app_home\WEB-INF, in all others it is a app_home. 
     *
     * @return the string
     */
    private String populateCodeRootPath()
    {
        String path = null;
        
        try
        {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            path = UrlUtils.decodeUrl(loader.getResource(
                    CONFIG_RESOURCE_PATH + "SystemConfig.class").getPath());
            
            if (Utils.isNothing(path))
                return getHome();
        }
        catch (Exception ex)
        {
            return getHome();
        }
        
        path = UrlUtils.urlToPath(path);
        if (Utils.isNothing(path))
            return getHome();
        
        if (!Utils.isWindows() && path.charAt(0) != '/')
            path = "/" + path;
        
        return path;
    }
    
    /**
     * Calculates home folder path.
     *
     * @return the string
     */
    private String populateHomePath()
    {
        String path = _sysProperties.get(HOME_PATH_PROPERTY);
        
        if (!Utils.isNothing(path))
            return FileUtils.getUnixFolderName(path);
        
        try
        {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            path = UrlUtils.decodeUrl(loader.getResource(
                    CONFIG_RESOURCE_PATH + "SystemConfig.class").getPath());
            
            if (Utils.isNothing(path))
                return FileUtils.getUnixFolderName(WORKING_PATH);
        }
        catch (Exception ex)
        {
            return FileUtils.getUnixFolderName(WORKING_PATH);
        }
        
        path = UrlUtils.urlToPath(path);
        if (Utils.isNothing(path))
            return FileUtils.getUnixFolderName(WORKING_PATH);
        
        int index = path.indexOf(UrlUtils.WEB_INF);
        
        if (index < 0)
            return FileUtils.getUnixFolderName(WORKING_PATH);
        
        int firstIndex = 0;
        if (path.charAt(0) == '\\')
            firstIndex = 1;
        
        path = FileUtils.getUnixFolderName(path.substring(firstIndex, index));
        
        if (!Utils.isWindows() && path.charAt(0) != '/')
            path = "/" + path;
        
        return path;
    }
    
    /**
     * Sets the path to all system folders.
     */
    private void setPath()
    {
        _home = populateHomePath();
        
        String codeRootPath = populateCodeRootPath();
        
        _libsPath = codeRootPath + LIB_PATH;
        
        _pluginsPath = (codeRootPath.indexOf(UrlUtils.WEB_INF) < 0) ? codeRootPath
                + PLUGIN_PATH
                : _libsPath;
        
        _settingsFolderName = FileUtils.getUnixFolderName(_home
                + CONFIG_FOLDER_PATH);
        
        _dataFolderName = FileUtils.getUnixFolderName(_home + DATA_FOLDER_PATH);
        
    }
    
    /**
     * Sets the system property.
     *
     * @param name the name of the property
     * @param value the value
     */
    public void setSystemProperty(String name, String value)
    {
        if (_sysProperties == null || name == null)
            return;
        
        _sysProperties.put(name, Utils.makeString(value).trim());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return getClass().getName();
    }
    
}
