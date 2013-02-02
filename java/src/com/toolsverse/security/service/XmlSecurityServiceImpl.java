/*
 * DefaultSecurityServiceImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.security.service;

import java.io.File;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.license.ClientCertificate;
import com.toolsverse.resource.Resource;
import com.toolsverse.security.DefaultSecurityCredentials;
import com.toolsverse.security.Role;
import com.toolsverse.security.SecurityContext;
import com.toolsverse.security.SecurityManager;
import com.toolsverse.security.SecurityModel;
import com.toolsverse.security.StoredPermission;
import com.toolsverse.security.resource.SecurityResource;
import com.toolsverse.util.Utils;
import com.toolsverse.util.XmlUtils;
import com.toolsverse.util.encryption.SymmetricEncryptor;
import com.toolsverse.util.log.Logger;

/**
 * The xml implementation of the SecurityService. Recommended to be used by
 * tests only.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */
public class XmlSecurityServiceImpl implements
        SecurityService<DefaultSecurityCredentials>
{
    
    /** The SECURITY_FOLDER. */
    private static final String SECURITY_FOLDER = "security";
    
    /** The USERS_CONFIG. */
    protected static final String USERS_CONFIG = "users.xml";
    
    /** The ROLES_CONFIG. */
    protected static final String ROLES_CONFIG = "roles.xml";
    
    // dictionary
    /** The USERS. */
    protected static final String USERS = "users";
    
    /** The USER. */
    protected static final String USER = "user";
    
    /** The PASSWORD. */
    protected static final String PASSWORD = "password";
    
    /** The ROLES. */
    protected static final String ROLES = "roles";
    
    /** The ROLE. */
    protected static final String ROLE = "role";
    
    /** The PERMISSIONS. */
    protected static final String PERMISSIONS = "permissions";
    
    /** The PERMISSION. */
    protected static final String PERMISSION = "permission";
    
    /** The NAME. */
    protected static final String NAME = "name";
    
    /** The ACTIONS. */
    protected static final String ACTIONS = "actions";
    
    /**
     * Gets the folder.
     * 
     * @return the folder
     */
    public String getFolder()
    {
        return SystemConfig.instance().getConfigFolderName() + SECURITY_FOLDER;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.service.SecurityService#getRoles(java.util.Map)
     */
    public Map<String, Role> getRoles(Map<String, String> defaultRoles)
        throws Exception
    {
        Map<String, Role> roles = new ConcurrentHashMap<String, Role>();
        
        Role roleObj = new Role(SecurityManager.ADMINISTRATOR);
        roles.put(SecurityManager.ADMINISTRATOR, roleObj);
        
        if (defaultRoles != null)
            for (String role : defaultRoles.keySet())
            {
                roleObj = new Role(role);
                roles.put(role, roleObj);
            }
        
        XmlUtils xml = null;
        
        File file = new File(getRolesFile());
        
        if (file != null && file.exists())
            xml = new XmlUtils(file);
        else
            return roles;
        
        Node rolesNode = xml.getFirstNodeNamed(ROLES);
        
        if (rolesNode == null)
            return roles;
        
        NodeList nodeList = rolesNode.getChildNodes();
        
        if (nodeList == null || nodeList.getLength() == 0)
            return roles;
        
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            Node node = nodeList.item(i);
            
            if ((node.getNodeType() == Node.TEXT_NODE)
                    || (node.getNodeType() == Node.COMMENT_NODE))
                continue;
            
            String role = xml.getStringAttribute(node, NAME);
            
            if (Utils.isNothing(role)
                    || SecurityManager.ADMINISTRATOR.equalsIgnoreCase(role)
                    || (defaultRoles != null && defaultRoles.containsKey(role)))
                continue;
            
            roleObj = new Role();
            
            roleObj.setName(role);
            
            roles.put(role, roleObj);
            
            Node permsNode = xml.getFirstNodeNamed(node, PERMISSIONS);
            if (permsNode == null)
                continue;
            
            NodeList permNodeList = permsNode.getChildNodes();
            
            if (permNodeList == null || permNodeList.getLength() == 0)
                continue;
            
            Map<String, StoredPermission> permissions = roleObj
                    .getPermissions();
            
            for (int index = 0; index < permNodeList.getLength(); index++)
            {
                Node permNode = permNodeList.item(index);
                
                if ((permNode.getNodeType() == Node.TEXT_NODE)
                        || (permNode.getNodeType() == Node.COMMENT_NODE))
                    continue;
                
                String name = xml.getStringAttribute(permNode, NAME);
                String actions = xml.getStringAttribute(permNode, ACTIONS);
                
                permissions.put(name, new StoredPermission(name, actions));
            }
        }
        
        return roles;
    }
    
    /**
     * Gets the roles file.
     * 
     * @return the roles file
     */
    protected String getRolesFile()
    {
        return getFolder() + "/" + ROLES_CONFIG;
    }
    
    /**
     * Gets the users file.
     * 
     * @return the users file
     */
    protected String getUsersFile()
    {
        return getFolder() + "/" + USERS_CONFIG;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.service.SecurityService#login(com.toolsverse.
     * security.SecurityCredentials)
     */
    public SecurityModel login(DefaultSecurityCredentials credentials)
        throws Exception
    {
        SecurityModel model = new SecurityModel();
        
        if (Utils.isNothing(credentials.getUserName()))
        {
            model.setMessage(SecurityResource.USER_NOT_FOUND.getValue());
            
            return model;
        }
        
        if (Utils.isNothing(credentials.getPassword()))
        {
            model.setMessage(SecurityResource.WRONG_PASSWORD.getValue());
            
            return model;
        }
        
        XmlUtils xml = null;
        
        File file = new File(getUsersFile());
        
        if (file != null && file.exists())
            xml = new XmlUtils(file);
        else
        {
            model.setMessage(SecurityResource.USER_NOT_FOUND.getValue());
            
            return model;
        }
        
        Node usersNode = xml.getFirstNodeNamed(USERS);
        
        if (usersNode == null)
        {
            model.setMessage(SecurityResource.USER_NOT_FOUND.getValue());
            
            return model;
        }
        
        NodeList nodeList = usersNode.getChildNodes();
        
        if (nodeList == null || nodeList.getLength() == 0)
        {
            model.setMessage(SecurityResource.USER_NOT_FOUND.getValue());
            
            return model;
        }
        
        for (int i = 0; i < nodeList.getLength(); i++)
        {
            Node node = nodeList.item(i);
            
            if ((node.getNodeType() == Node.TEXT_NODE)
                    || (node.getNodeType() == Node.COMMENT_NODE))
                continue;
            
            String user = xml.getStringAttribute(node, NAME);
            
            if (credentials.getUserName().equalsIgnoreCase(user))
            {
                String password = xml.getStringAttribute(node, PASSWORD);
                
                if (Utils.isNothing(password))
                {
                    model.setMessage(SecurityResource.WRONG_PASSWORD.getValue());
                    
                    return model;
                }
                
                try
                {
                    password = SymmetricEncryptor.decryptPassword(
                            ClientCertificate.instance().getKey(), password);
                }
                catch (GeneralSecurityException ex)
                {
                    Logger.log(Logger.INFO, this,
                            Resource.ERROR_DECRYPTING.getValue(), ex);
                    
                    model.setMessage(SecurityResource.WRONG_PASSWORD.getValue());
                    
                    return model;
                    
                }
                
                if (password.equals(credentials.getPassword()))
                {
                    Node rolesNode = xml.getFirstNodeNamed(node, ROLES);
                    
                    if (rolesNode == null)
                    {
                        model.setMessage(SecurityResource.USER_HAS_NO_PERMISSIONS
                                .getValue());
                        
                        return model;
                    }
                    
                    NodeList rolesNodeList = rolesNode.getChildNodes();
                    
                    if (rolesNodeList == null || rolesNodeList.getLength() == 0)
                    {
                        model.setMessage(SecurityResource.USER_HAS_NO_PERMISSIONS
                                .getValue());
                        
                        return model;
                    }
                    
                    Set<String> roles = new HashSet<String>();
                    
                    for (int index = 0; index < rolesNodeList.getLength(); index++)
                    {
                        Node roleNode = rolesNodeList.item(index);
                        
                        if ((roleNode.getNodeType() == Node.TEXT_NODE)
                                || (roleNode.getNodeType() == Node.COMMENT_NODE))
                            continue;
                        
                        String role = xml.getStringAttribute(roleNode, NAME);
                        
                        if (!Utils.isNothing(role))
                            roles.add(role);
                    }
                    
                    if (roles.size() == 0)
                    {
                        model.setMessage(SecurityResource.USER_HAS_NO_PERMISSIONS
                                .getValue());
                        
                        return model;
                    }
                    
                    SecurityContext context = new SecurityContext();
                    
                    context.setRoles(roles);
                    context.setCredentials(credentials);
                    
                    model.setSecurityContext(context);
                    
                    return model;
                }
                else
                {
                    model.setMessage(SecurityResource.WRONG_PASSWORD.getValue());
                    
                    return model;
                }
                
            }
        }
        
        model.setMessage(SecurityResource.USER_NOT_FOUND.getValue());
        
        return model;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.security.service.SecurityService#logout(com.toolsverse
     * .security.SecurityContext)
     */
    public boolean logout(SecurityContext context)
        throws Exception
    {
        return true;
    }
    
}
