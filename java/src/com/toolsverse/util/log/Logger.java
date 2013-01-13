/*
 * Logger.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.log;

import com.toolsverse.util.factory.ObjectFactory;

/**
 * A Logger object is used to log messages for a specific
 * system or application component.  Loggers are normally named,
 * using a hierarchical dot-separated namespace.  Logger names
 * can be arbitrary strings, but they should normally be based on
 * the package name or class name of the logged component, such
 * as java.net or javax.swing.  In addition it is possible to create
 * "anonymous" Loggers that are not stored in the Logger namespace. 
 * Logger uses configurable implementation of the <code>LogWriter</code> to do actual job.  
 *
 * @see com.toolsverse.util.log.LogWriter
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Logger
{
    /**
     * Informational only - no help desk or support is required. Typically used
     * to display startup information, such as in the init() method of a
     * servlet. Use sparingly.
     */
    public final static int INFO = 0;
    
    /**
     * For both Development and Production debugging. This level MUST be used
     * conditionally, after invoking "isDebugEnabled()".
     */
    public final static int DEBUG = 1;
    
    /**
     * A recoverable error that will likely not need help desk or immediate
     * development support. Example, a connection that could not be established
     * on the first try, but could be recovered. This type of error may impact
     * the user, but they can likely recover or find a workaround.
     */
    public final static int WARNING = 2;
    
    /**
     * An error that lets the application continue running, but needs to be
     * fixed. The user cannot continue normal business operations without this
     * error being fixed.
     */
    public final static int SEVERE = 3;
    
    /**
     * An error that renders the application useless, such as a "FileNotFound"
     * exception on a file that contains startup information.
     */
    public final static int FATAL = 4;
    
    public final static int NO_TYPE = -0xFFFF;
    
    /**
     * Default LogWriter
     */
    private static LogWriter _logger = (LogWriter)ObjectFactory.instance().get(
            LogWriter.class.getName(), Log4jWriter.class.getName(),
            LogWriter.class, false);
    
    /**
     * Return a reference to the <code>LogWriter</code> implementation. You will not normally
     * need to invoke this unless you want to invoke methods on the
     * implementation that are not on the LogWriter interface. (You'll need to
     * cast it back to the underlying implementation).
     */
    public static LogWriter getLogger()
    {
        return _logger;
    }
    
    /**
     * Logs a message.
     * 
     * @param logType
     *            The type of log that is being written. Common types are
     *            "info", "debug", "warning", "fatal", etc.
     * @param object
     *            The object that is writing the message. If the object is a
     *            static class (such as Logger), pass the Class object instead -
     *            e.g., "Logger.class". Null is acceptable.
     * @param message
     *            The actual message to write to the log. Null is acceptable,
     *            but why would you?
     * @return The exact message that was written to the log. This is a
     *         concatenation of the current time, the log type, the name of the
     *         object that is writing to the log, the method name from which the
     *         log was written, the message, and if applicable, the throwable
     *         (and any nested throwables) and their stack traces.
     */
    
    public static String log(int logType, Object object, String message)
    {
        return log(logType, object, message, null);
    }
    
    /**
     * Logs a message.
     * 
     * @param logType
     *            The type of log that is being written. Common types are
     *            "info", "debug", "warning", "fatal", etc.
     * @param object
     *            The object that is writing the message. If the object is a
     *            static class (such as Logger), pass the Class object instead -
     *            e.g., "Logger.class". Null is acceptable.
     * @param message
     *            The actual message to write to the log. Null is acceptable,
     *            but why would you?
     * @param throwable
     *            Any Throwable whose contents you want written with the log
     *            message. The Throwable's cause is check recursively and
     *            displays all causes in an abbreviated format.
     * @return The exact message that was written to the log. This is a
     *         concatenation of the current time, the log type, the name of the
     *         object that is writing to the log, the method name from which the
     *         log was written, the message, and if applicable, the throwable
     *         (and any nested throwables) and their stack traces.
     */
    public static String log(int logType, Object object, String message,
            Throwable throwable)
    {
        return _logger.log(logType, object, message, throwable);
    }
    
    /**
     * Set the <code>LogWriter</code>  
     * 
     * @param logger
     *            Any implementation of LogWriter. This method lets you swap out
     *            the default LogWriter - which simply writes to the console -
     *            with LogWriters that write to files, databases, central JNDI
     *            components that log for all running instances of a server app,
     *            etc. Null is not allowed.
     */
    public synchronized static void setLogger(LogWriter logger)
    {
        if (logger == null)
            throw new IllegalArgumentException(Logger.class.getName()
                    + ".setLogger(LogWriter logger) - logger cannot be null");
        
        _logger = logger;
    }
    
    /**
     * Creates and instance of the Logger.   
     */
    private Logger()
    {
    }
    
}
