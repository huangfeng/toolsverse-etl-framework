/*
 * WebSessionManager.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service.web;

import javax.servlet.http.HttpSession;

/**
 * A global way to access the current http session.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public final class WebSessionManager
{
    
    /** The currentSession. */
    private static final ThreadLocal<HttpSession> currentSession = new InheritableThreadLocal<HttpSession>();
    
    /**
     * Get the Session that is currently associated with this Thread.
     * 
     * @return the Session
     */
    public static HttpSession getSession()
    {
        return currentSession.get();
    }
    
    /**
     * Removes the session.
     */
    public static void removeSession()
    {
        currentSession.set(null);
    }
    
    /**
     * Associate the Session with the current Thread. 
     * 
     * @param session
     *            the Session
     */
    public static void setSession(HttpSession session)
    {
        currentSession.set(session);
    }
}
