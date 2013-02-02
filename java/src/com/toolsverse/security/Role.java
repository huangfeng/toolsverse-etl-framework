/*
 * Role.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import java.util.List;

import com.toolsverse.mvc.model.Getter;
import com.toolsverse.mvc.model.ModelImpl;
import com.toolsverse.mvc.model.Setter;
import com.toolsverse.util.IndexArrayList;
import com.toolsverse.util.ListHashMap;

/**
 * The security subsystem provides an access to data or resources based on credentials supplied by the user. 
 * It checks the roles of a user and provides access to resources based on these roles. Role includes all associated permissions.  
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class Role extends ModelImpl
{
    // dictionary
    /** The ROLE_NAME. */
    public static final String ROLE_NAME = "rolename";
    
    /** The ROLE_PERMISSIONS. */
    public static final String ROLE_PERMISSIONS = "rolepermissions";
    
    /**
     * Instantiates a new role.
     */
    public Role()
    {
        this(null);
    }
    
    /**
     * Instantiates a new role using given name
     *
     * @param name the name
     */
    public Role(String name)
    {
        super();
        
        setPermissions(new ListHashMap<String, StoredPermission>()
        {
            @Override
            public List<StoredPermission> createList()
            {
                return new IndexArrayList<StoredPermission>();
            }
        });
        
        setName(name);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Role other = (Role)obj;
        if (getName() == null)
        {
            if (other.getName() != null)
                return false;
        }
        else if (!getName().equals(other.getName()))
            return false;
        return true;
    }
    
    /**
     * Gets the name.
     *
     * @return the name
     */
    @Getter(name = ROLE_NAME)
    public String getName()
    {
        return (String)getAttributeValue(ROLE_NAME);
    }
    
    /**
     * Gets the permissions.
     *
     * @return the permissions
     */
    @SuppressWarnings("unchecked")
    @Getter(name = ROLE_PERMISSIONS)
    public ListHashMap<String, StoredPermission> getPermissions()
    {
        return (ListHashMap<String, StoredPermission>)getAttributeValue(ROLE_PERMISSIONS);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((getName() == null) ? 0 : getName().hashCode());
        return result;
    }
    
    /**
     * Sets the name.
     *
     * @param value the new name
     */
    @Setter(name = ROLE_NAME)
    public void setName(String value)
    {
        setAttributeValue(ROLE_NAME, value);
    }
    
    /**
     * Sets the permissions.
     *
     * @param value the value
     */
    private void setPermissions(ListHashMap<String, StoredPermission> value)
    {
        setAttributeValue(ROLE_PERMISSIONS, value);
    }
    
}
