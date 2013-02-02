/*
 * StoredPermission.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import java.security.BasicPermission;
import java.security.Permission;

import com.toolsverse.util.Utils;

/**
 * The permission which is stored in the permanent storage (xml, database) as a pattern and retrieved by the security subsystem to compare with the
 * list of permissions associated with the user.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public final class StoredPermission extends BasicPermission
{
    
    /** The actions. */
    private String _actions;
    
    /**
     * Instantiates a new stored permission.
     *
     * @param perm the perm
     */
    public StoredPermission(Permission perm)
    {
        super(perm.getName(), perm.getActions());
    }
    
    /**
     * Instantiates a new stored permission.
     *
     * @param name the name
     */
    public StoredPermission(String name)
    {
        this(name, null);
    }
    
    /**
     * Instantiates a new stored permission.
     *
     * @param name the name
     * @param actions the actions
     */
    public StoredPermission(String name, String actions)
    {
        super(name);
        
        _actions = actions;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.security.BasicPermission#getActions()
     */
    @Override
    public String getActions()
    {
        return _actions;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.security.BasicPermission#implies(java.security.Permission)
     */
    @Override
    public boolean implies(Permission permission)
    {
        boolean ret = permission.getName().equalsIgnoreCase(getName());
        
        if (!ret || Utils.isNothing(permission.getActions()))
            return ret;
        
        String permActions = permission.getActions().trim().toLowerCase();
        String compareToActions = Utils.makeString(getActions()).trim()
                .toLowerCase();
        
        return compareToActions.indexOf(permActions) >= 0;
    }
    
    /**
     * Sets the actions.
     *
     * @param value the new actions
     */
    public void setActions(String value)
    {
        _actions = value;
    }
}
