/*
 * DefaultSecurityProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import java.security.Permission;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.toolsverse.util.TypedKeyValue;

/**
 * The default implementation of the SecurityProvider.
 *
 * @see com.toolsverse.security.SecurityContext
 * @see com.toolsverse.security.Role
 * @see com.toolsverse.security.SecuritySubject
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public class DefaultSecurityProvider implements SecurityProvider
{
    
    /** The permissions. */
    private Map<String, Permission> _permissions;
    
    /** The roles. */
    private Map<String, Role> _roles;
    
    /**
     * Instantiates a new default security provider.
     */
    public DefaultSecurityProvider()
    {
        _permissions = new LinkedHashMap<String, Permission>();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.PermissionsProvider#addPermission(java.security
     * .Permission)
     */
    public void addPermission(Permission permission)
    {
        _permissions.put(permission.getName(), permission);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.PermissionsProvider#clearPermissions()
     */
    public void clearPermissions()
    {
        _permissions.clear();
    }
    
    /**
     * Checks if SecurityContext contains permission.
     *
     * @param permissionName the permission name
     * @param context the SecurityContext
     * @param checkPerm the check permission flag. 
     * @return the KeyValue object where key is true/false and value is StoredPermission
     */
    private TypedKeyValue<Boolean, StoredPermission> containsPermission(
            String permissionName, SecurityContext context, boolean checkPerm)
    {
        TypedKeyValue<Boolean, StoredPermission> ret = new TypedKeyValue<Boolean, StoredPermission>(
                false, null);
        
        if (_roles == null || permissionName == null || context == null)
            return ret;
        
        if (context.isAdministrator()
                || (checkPerm && !_permissions.containsKey(permissionName)))
        {
            ret.setKey(true);
            
            return ret;
        }
        
        Set<String> roles = context.getRoles();
        
        if (roles == null || roles.size() == 0)
            return ret;
        
        for (String role : roles)
        {
            Role roleObj = _roles.get(role);
            
            if (roleObj != null && roleObj.getPermissions() != null
                    && roleObj.getPermissions().containsKey(permissionName))
            {
                ret.setKey(true);
                ret.setValue(roleObj.getPermissions().get(permissionName));
                
                return ret;
            }
        }
        
        return ret;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.PermissionsProvider#deletePermission(java.security
     * .Permission)
     */
    public void deletePermission(Permission permission)
    {
        _permissions.remove(permission.getName());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.PermissionsProvider#getPermissions()
     */
    public Map<String, Permission> getPermissions()
    {
        return _permissions;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.SecurityProvider#getRoles()
     */
    public Map<String, Role> getRoles()
    {
        return _roles;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.SecurityProvider#hasRole(java.lang.String,
     * com.toolsverse.security.SecurityContext)
     */
    public boolean hasRole(String role, SecurityContext context)
    {
        if (context == null || role == null)
            return false;
        
        if (context.isAdministrator() && _roles.containsKey(role))
            return true;
        
        Set<String> roles = context.getRoles();
        
        return roles != null && roles.contains(role);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.SecurityProvider#isPermitted(java.security.Permission
     * , com.toolsverse.security.SecurityContext)
     */
    public boolean isPermitted(Permission permission, SecurityContext context)
    {
        if (permission == null || context.isAdministrator())
            return true;
        
        if (permission instanceof DefaultPermission
                && ((DefaultPermission)permission).isAdminOnly())
        {
            if (!context.isAdministrator())
                return false;
        }
        
        TypedKeyValue<Boolean, StoredPermission> contains = containsPermission(
                permission.getName(), context, false);
        
        if (!contains.getKey())
            return false;
        
        return contains.getValue().implies(permission);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.SecurityProvider#isPermitted(com.toolsverse.security
     * .SecuritySubject, com.toolsverse.security.SecurityContext)
     */
    public boolean isPermitted(SecuritySubject subject, SecurityContext context)
    {
        if (subject == null)
            return false;
        
        if (subject.getPermission() == null || context.isAdministrator())
            return true;
        
        TypedKeyValue<Boolean, StoredPermission> contains = containsPermission(
                subject.getPermission().getName(), context, false);
        
        if (!contains.getKey())
            return false;
        
        Permission subjectPermission = subject.getPermission();
        
        Permission permission = _permissions.get(subjectPermission.getName());
        
        if (permission == null)
            return false;
        
        if (permission instanceof DefaultPermission
                && ((DefaultPermission)permission).isAdminOnly())
        {
            if (!context.isAdministrator())
                return false;
        }
        
        return permission.implies(subjectPermission);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.SecurityProvider#isPermitted(java.lang.String,
     * com.toolsverse.security.SecurityContext)
     */
    public boolean isPermitted(String permissionName, SecurityContext context)
    {
        return containsPermission(permissionName, context, true).getKey();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.security.SecurityProvider#setRoles(java.util.Map)
     */
    public void setRoles(Map<String, Role> value)
    {
        _roles = value;
    }
}
