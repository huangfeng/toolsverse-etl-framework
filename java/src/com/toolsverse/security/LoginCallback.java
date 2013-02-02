/*
 * LoginCallback.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

/**
 * This is a callback interface which must be implemented by the class which does something after login attempt. 
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public interface LoginCallback
{
    
    /**
     * Fired after login attempt.
     *
     * @param securityMode the security mode
     */
    void loginCalback(SecurityModel securityMode);
}
