/*
 * SecuritySubject.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security;

import java.security.Permission;

/**
 * The wrapper interface for the Permission and possible actions, associated with the Permission. For example Permission define an access to the particular
 * node in the tree and the possible actions are: edit, delete, etc.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public interface SecuritySubject
{
    
    /**
     * Gets the actions.
     *
     * @return the actions
     */
    String getActions();
    
    /**
     * Gets the permission.
     *
     * @return the permission
     */
    Permission getPermission();
}
