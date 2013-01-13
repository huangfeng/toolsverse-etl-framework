/*
 * LogWriter.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.log;

/**
 * LogWriter is an interface which each particular log writer class must implement. 
 *
 * @see com.toolsverse.util.log.Log4jWriter 
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface LogWriter
{
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
    String log(int logType, Object object, String message, Throwable throwable);
    
    /**
     * Sets the log level for the object.
     * 
     * @param object The object that is writing the message.
     * @param level The type of log that is being written. Common types are "info", "debug", "warning", "fatal", etc.
     */
    void setLevel(Object object, int level);
}