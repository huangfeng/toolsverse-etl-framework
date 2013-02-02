/*
 * DefaultPermission.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import java.security.BasicPermission;

/**
 * The base class for all type of permissions.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class DefaultPermission extends BasicPermission
{
    
    /** The title. */
    private String _title;
    
    /**
     * Instantiates a new default permission.
     *
     * @param title the title
     * @param name the name
     */
    public DefaultPermission(String title, String name)
    {
        super(name);
        _title = title;
    }
    
    /**
     * Gets the default actions.
     *
     * @return the default actions
     */
    public String getDefaultActions()
    {
        return null;
    }
    
    /**
     * Gets the title.
     *
     * @return the title
     */
    public String getTitle()
    {
        return _title;
    }
    
    /**
     * Gets the type.
     *
     * @return the type
     */
    public abstract String getType();
    
    /**
     * Checks if permission can be associated with the administrator user only.
     *
     * @return true, if permission can be associated with the administrator user only
     */
    public abstract boolean isAdminOnly();
    
    /*
     * (non-Javadoc)
     * 
     * @see java.security.Permission#toString()
     */
    @Override
    public String toString()
    {
        return _title != null ? _title : getName();
    }
}
