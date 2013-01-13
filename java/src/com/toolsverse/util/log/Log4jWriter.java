/*
 * Log4jWriter.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.log;

import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.util.Utils;

/**
 * Log4jWriter is a log4j implementation of the <code>LogWriter</code> interface.
 *
 * @see com.toolsverse.util.log.LogWriter
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Log4jWriter implements LogWriter
{
    
    /**
     * Instantiates a new Log4jWriter
     */
    public Log4jWriter()
    {
        Properties properties = SystemConfig.instance().loadPropsFromFile(
                SystemConfig.instance().getConfigFolderName()
                        + "log4j.properties");
        
        String home = SystemConfig.instance().getHome();
        String appName = SystemConfig.instance().getAppName();
        
        if (properties != null && properties.size() > 0)
        {
            for (Object name : properties.keySet())
            {
                String value = properties.getProperty(name.toString());
                
                if (value != null)
                {
                    
                    value = Utils
                            .findAndReplaceRegexp(value, "{"
                                    + SystemConfig.HOME_PATH_PROPERTY + "}",
                                    home, true);
                    
                    value = Utils.findAndReplaceRegexp(value, "{"
                            + SystemConfig.APP_NAME + "}", appName, true);
                    
                    properties.setProperty(name.toString(), value);
                }
            }
            
            PropertyConfigurator.configure(properties);
        }
    }
    
    /**
     * Appends the text of the throwable and it's stack trace, including and
     * nested throwables.
     *
     * @param log the log
     * @param throwable the throwable
     */
    private void appendThrowable(StringBuffer log, Throwable throwable)
    {
        log.append(Utils.NEWLINE);
        
        String subClass = "Throwable";
        
        if (throwable instanceof Error)
            subClass = "Error";
        else if (throwable instanceof Exception)
            subClass = "Exception";
        
        log.append(Utils.NEWLINE).append(subClass).append(" ");
        
        String stackTrace = Utils.getStackTraceAsString(throwable);
        
        log.append(Utils.NEWLINE).append(stackTrace);
    }
    
    /**
     * Find the java.util.logging.Logger level corresponding to one of our
     * logging levels
     *
     * @param level the level
     * @return the level
     * @returns Level
     */
    private Level getLevel(int level)
    {
        Level rtn = Level.OFF;
        
        switch (level)
        {
            case Logger.INFO:
            {
                rtn = Level.INFO;
                break;
            }
            
            case Logger.DEBUG:
            {
                rtn = Level.DEBUG;
                break;
            }
            
            case Logger.WARNING:
            {
                rtn = Level.WARN;
                break;
            }
            
            case Logger.SEVERE:
            {
                rtn = Level.ERROR;
                break;
            }
            
            case Logger.FATAL:
            {
                rtn = Level.FATAL;
                break;
            }
            
        }
        
        return rtn;
    }
    
    /**
     * Creates a log message.
     *
     * @param logType The type of log that is being written. Common types are
     * "info", "debug", "warning", "fatal", etc.
     * @param object The object that is writing the message. If the object is a
     * static class (such as Logger), pass the Class object instead -
     * e.g., "Logger.class". Null is acceptable.
     * @param message The actual message to write to the log. Null is acceptable,
     * but why would you?
     * @param throwable Any Throwable whose contents you want written with the log
     * message. The Throwable's cause is check recursively and
     * displays all causes in an abbreviated format.
     * @return The exact message that was written to the log. This is a
     * concatenation of the current time, the log type, the name of the
     * object that is writing to the log, the method name from which the
     * log was written, the message, and if applicable, the throwable
     * (and any nested throwables) and their stack traces.
     */
    private String getLogEntry(int logType, Object object, String message,
            Throwable throwable)
    {
        String logEntry = null;
        
        StringBuffer log = new StringBuffer(500);
        
        if (message != null)
            log.append(message);
        
        if (throwable != null)
            appendThrowable(log, throwable);
        
        logEntry = log.toString();
        
        return logEntry;
    }
    
    /**
     * Gets the log4j logger for the object 
     *
     * @param object the object
     * @return the logger
     */
    public org.apache.log4j.Logger getLogger(Object object)
    {
        return org.apache.log4j.Logger.getLogger(getObjectClassName(object));
    }
    
    /**
     * Returns:
     * <p>
     * (Class<?>)object).getName() if object is a class
     * <p> 
     * object.getClass().getName() of object is not null and not a class
     * <p>
     * "root" if object is null
     *
     * @param object the object
     * @return the object class name
     */
    private String getObjectClassName(Object object)
    {
        return ((object != null) ? ((object instanceof Class) ? ((Class<?>)object)
                .getName() : object.getClass().getName())
                : "root");
    }
    
    /**
     * Checks if log level is enabled for the logger
     *
     * @param level the level
     * @param logger the logger
     * @return true, if is enabled
     */
    private boolean isEnabled(Level level, org.apache.log4j.Logger logger)
    {
        return logger != null ? logger.isEnabledFor(level) : false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.log.LogWriter#log(int, java.lang.Object,
     * java.lang.String, java.lang.Throwable)
     */
    public String log(int logType, Object object, String message,
            Throwable throwable)
    {
        org.apache.log4j.Logger logger = getLogger(object);
        
        Level level = getLevel(logType);
        
        if (!isEnabled(level, logger))
            return null;
        
        String logEntry = getLogEntry(logType, object, message, throwable);
        
        logger.log(level, logEntry);
        
        return logEntry;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.log.LogWriter#setLevel(java.lang.Object, int)
     */
    public void setLevel(Object object, int level)
    {
        org.apache.log4j.Logger logger = getLogger(object);
        
        if (logger != null)
            logger.setLevel(getLevel(level));
    }
    
}
