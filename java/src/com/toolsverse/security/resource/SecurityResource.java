/*
 * SecurityResource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security.resource;

import com.toolsverse.config.SystemConfig;

/**
 * The messages used by the security framework.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public enum SecurityResource
{
    // messages
    SECRET_KEY_IS_WEAK("Secret key is weak."),
    
    ERROR_DECRIPTING("Error decrypting string."),
    
    USER_NOT_FOUND("The user name or password you entered is incorrect."),
    
    USER_HAS_NO_PERMISSIONS(
            "User has no permissions. Please enter different name and try again."),
    
    CANNOT_LOGIN("Cannot login."),
    
    CANNOT_LOGOUT("Cannot logout."),
    
    CANNOT_INIT_NO_SERVICE(
            "Cannot initialize security manager. The security service not found."),
    
    CANNOT_INIT("Cannot initialize security manager."),
    
    CANNOT_INIT_NO_PROVIDER(
            "Cannot initialize security manager. The security provider not found."),
    
    WRONG_PASSWORD("The user name or password you entered is incorrect."),
    
    PASSWORDS_DID_NOT_MATCH("The passwords you entered did not match."),
    
    WRONG_PASSWORD_ONLY("The password you entered is incorrect.");
    
    /** The _value. */
    private String _value;
    
    /**
     * Instantiates a new security resource.
     * 
     * @param value
     *            the value
     */
    SecurityResource(String value)
    {
        _value = value;
    }
    
    /**
     * Gets the value of this enum constant.
     * 
     * @return the value
     */
    public String getValue()
    {
        return SystemConfig.instance().getObjectProperty(this);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString()
    {
        return _value;
    }
}
