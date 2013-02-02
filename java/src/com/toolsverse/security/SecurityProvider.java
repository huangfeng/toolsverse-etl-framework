/*
 * SecurityProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import java.security.Permission;
import java.util.Map;

/**
 * This class checks role-based permissions associated with the given security context.
 *
 * @see com.toolsverse.security.SecurityContext
 * @see com.toolsverse.security.SecuritySubject
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public interface SecurityProvider extends PermissionsProvider
{
    
    /**
     * Gets the roles.
     *
     * @return the roles
     */
    Map<String, Role> getRoles();
    
    /**
     * Checks if user has a given role.
     *
     * @param role the role
     * @param context the context
     * @return true, if successful
     */
    boolean hasRole(String role, SecurityContext context);
    
    /**
     * Checks if permission is permitted for the user identified by the context.
     *
     * @param permission the permission
     * @param context the context
     * @return true, if is permitted
     */
    boolean isPermitted(Permission permission, SecurityContext context);
    
    /**
     * Checks if subject is permitted for the user identified by the context.
     *
     * @param subject the security subject
     * @param context the security context 
     * @return true, if is permitted
     */
    boolean isPermitted(SecuritySubject subject, SecurityContext context);
    
    /**
     * Checks if permission is permitted for the user identified by the context.
     *
     * @param permissionName the permission name
     * @param context the context
     * @return true, if is permitted
     */
    boolean isPermitted(String permissionName, SecurityContext context);
    
    /**
     * Sets the roles.
     *
     * @param value the value
     */
    void setRoles(Map<String, Role> value);
}
