/*
 * SecurityModel.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import java.io.Serializable;

import com.toolsverse.util.Utils;

/**
 * The login service must return an instance of the SecurityModel class. It includes SecurityContext or an error message if login is failed.
 *
 * @see com.toolsverse.security.SecurityContext
 * @see com.toolsverse.security.service.SecurityService
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public class SecurityModel implements Serializable
{
    
    /** The context. */
    private SecurityContext _context;
    
    /** The message. */
    private String _message;
    
    /**
     * Instantiates a new security model.
     */
    public SecurityModel()
    {
        _context = null;
        _message = null;
    }
    
    /**
     * Gets the message.
     *
     * @return the message
     */
    public String getMessage()
    {
        return _message;
    }
    
    /**
     * Gets the security context.
     *
     * @return the security context
     */
    public SecurityContext getSecurityContext()
    {
        return _context;
    }
    
    /**
     * Checks if both message and context are null.
     *
     * @return true, if is empty
     */
    public boolean isEmpty()
    {
        return Utils.isNothing(_message) && _context == null;
    }
    
    /**
     * Sets the message.
     *
     * @param value the new message
     */
    public void setMessage(String value)
    {
        _message = value;
    }
    
    /**
     * Sets the security context.
     *
     * @param value the new security context
     */
    public void setSecurityContext(SecurityContext value)
    {
        _context = value;
    }
}
