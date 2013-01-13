/*
 * TreeNode.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * An abstract class to store tree-like structure where there is a root node and each node might have
 * a list of children. Node which can have children called FOLDER and node which can not - ITEM.
 * The example would be a file system where directories are folders and files are items.   
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class TreeNode implements Serializable, Cloneable
{
    /** FOLDER */
    public static int FOLDER_TYPE = 0;
    
    /** ITEM which is not FOLDER */
    public static int ITEM_TYPE = 1;
    
    private int _type;
    
    private List<TreeNode> _children;
    
    private TreeNode _parent;
    
    private transient PropertyChangeSupport _propertyChangeSupport;
    
    /**
     * Instantiates a new empty TreeNode.
     */
    public TreeNode()
    {
        _type = ITEM_TYPE;
        
        _children = null;
        _parent = null;
        
        _propertyChangeSupport = new PropertyChangeSupport(this);
    }
    
    /**
     * Adds the child node to the end of the list
     * 
     * @param node the child node to add
     * 
     * @return the index of the child node
     */
    public int addNode(TreeNode node)
    {
        return insertNode(node, _children == null ? 0 : _children.size());
    }
    
    /**
     * Adds the property change listener.
     * 
     * @param listener the listener
     */
    public void addPropertyChangeListener(PropertyChangeListener listener)
    {
        if (_propertyChangeSupport == null)
            _propertyChangeSupport = new PropertyChangeSupport(this);
        
        _propertyChangeSupport.addPropertyChangeListener(listener);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#clone()
     */
    @Override
    public Object clone()
    {
        TreeNode newNode = createNode(this);
        
        if (_children != null && _children.size() > 0)
        {
            List<TreeNode> items = new ArrayList<TreeNode>();
            
            newNode.setChildren(items);
            
            for (TreeNode node : _children)
            {
                TreeNode child = (TreeNode)node.clone();
                
                child.setParent(newNode);
                
                items.add(child);
            }
        }
        
        return newNode;
    }
    
    /**
     * Creates the node from the specified node
     * 
     * @param node the TreeNode which used to create new node 
     * 
     * @return the new TreeNode
     */
    public abstract TreeNode createNode(TreeNode node);
    
    /**
     * Deletes this node from the parent. 
     * 
     * @return the previous index of the node or if this node didn't have a parent or has not been found
     * in the children list returns null.    
     */
    public int deleteNode()
    {
        if (_parent == null || _parent.getChildren() == null)
            return -1;
        
        int index = _parent.getChildren().indexOf(this);
        
        if (index < 0)
            return -1;
        
        _parent.getChildren().remove(index);
        
        int ret;
        
        if (index < _parent.getChildren().size())
            ret = index;
        else if (_parent.getChildren().size() == 0)
            ret = -1;
        else
            ret = 0;
        
        return ret;
    }
    
    /**
     * Gets the list of the children nodes.
     * 
     * @return the list of the children nodes
     */
    public List<TreeNode> getChildren()
    {
        return _children;
    }
    
    /**
     * Gets the display value of the node. Most likely this value will be displayed in the 
     * JTree or similar component.
     * 
     * @return the display value of the node 
     */
    public abstract String getDisplayValue();
    
    /**
     * Gets the icon path. Most likely icon will be displayed in the 
     * JTree or similar component.
     * 
     * @return the icon path if node has an icon otherwise returns null
     */
    public abstract String getIconPath();
    
    /**
     * Gets the child TreeNode located at the index. If node has no children or index is 
     * out of bounds returns null. 
     * 
     * @param index the index of the child node
     * 
     * @return the child TreeNode located at the index  
     */
    public TreeNode getNode(int index)
    {
        if (_children == null || _children.size() == 0
                || _children.size() <= index || index < 0)
            return null;
        
        return _children.get(index);
    }
    
    /**
     * Gets the parent TreeNode if any. 
     * 
     * @return the parent TreeNode and null if there is none
     */
    public TreeNode getParent()
    {
        return _parent;
    }
    
    /**
     * Gets the PropertyChangeSupport object. Can be used by controller to subscribe on change events, 
     * such as current node change, etc.     
     * 
     * @return the associated PropertyChangeSupport object
     */
    public PropertyChangeSupport getPropertyChangeSupport()
    {
        return _propertyChangeSupport;
    }
    
    /**
     * Gets the root node of the tree. The root node is a node without parent.      
     * 
     * @return the root TreeNode
     */
    public TreeNode getRoot()
    {
        if (getParent() == null)
            return this;
        else
            return getParent().getRoot();
    }
    
    /**
     * Gets the type of the node. Possible types: ITEM or FOLDER
     * 
     * @return the type of the node.
     */
    public int getType()
    {
        return _type;
    }
    
    /**
     * Gets the index of this node in the list of the parents children. If there is no parent or node can 
     * not be found returns -1.
     * 
     * @return the index of the node
     */
    public int indexOf()
    {
        if (_parent == null || _parent.getChildren() == null)
            return -1;
        
        return _parent.getChildren().indexOf(this);
        
    }
    
    /**
     * Inserts child node at the specified index. Sets parent of the child node to this node.
     * 
     * @param node the child node to insert
     * @param index the position to insert child node at 
     * 
     * @return the new index of the child node
     */
    public int insertNode(TreeNode node, int index)
    {
        if (!isFolder())
        {
            return getParent().insertNode(node, indexOf());
        }
        
        if (_children == null)
            _children = new ArrayList<TreeNode>();
        
        _children.add(index, node);
        
        node.setParent(this);
        
        return index;
    }
    
    /**
     * Checks if is this node is a FOLDER.
     * 
     * @return true, if node is a FOLDER
     */
    public boolean isFolder()
    {
        return FOLDER_TYPE == _type;
    }
    
    /**
     * Checks if this node is an ITEM.
     * 
     * @return true, if node is an ITEM
     */
    public boolean isItem()
    {
        return ITEM_TYPE == _type;
    }
    
    /**
     * Notifies all attached listeners when tree's current node has changed.   
     * 
     * @param event the event
     */
    public void propertyChange(PropertyChangeEvent event)
    {
        PropertyChangeListener[] listeners = _propertyChangeSupport
                .getPropertyChangeListeners();
        
        if (listeners == null)
            return;
        
        for (PropertyChangeListener listener : listeners)
            listener.propertyChange(event);
    }
    
    /**
     * Removes the PropertyChangeListener from the list of attached listeners.
     * 
     * @param listener the listener to remove
     */
    public void removePropertyChangeListener(PropertyChangeListener listener)
    {
        if (_propertyChangeSupport != null)
            _propertyChangeSupport.removePropertyChangeListener(listener);
    }
    
    /**
     * Sets the list of the children to this node.  
     * 
     * @param value the new children list
     */
    public void setChildren(List<TreeNode> value)
    {
        _children = value;
    }
    
    /**
     * Sets the current node for the tree. Notifies all attached listeners that current node has changed. 
     * 
     * @param treeNode the new current node
     */
    public void setCurrentNode(TreeNode treeNode)
    {
        TreeNode root = treeNode.getRoot();
        
        if (root != null)
        {
            root.propertyChange(new PropertyChangeEvent(this, "currentnode",
                    null, treeNode));
        }
    }
    
    /**
     * Replaces this node with a new node. 
     * 
     * @param node the new node
     */
    public void setNode(TreeNode node)
    {
        int index = indexOf();
        
        if (index < 0)
            return;
        
        node.setParent(getParent());
        
        getParent().getChildren().set(index, node);
    }
    
    /**
     * Sets the parent node for this node.
     * 
     * @param value the new parent node
     */
    public void setParent(TreeNode value)
    {
        _parent = value;
    }
    
    /**
     * Sets the PropertyChangeSupport object for the node.   
     * 
     * @param value the new PropertyChangeSupport object
     */
    public void setPropertyChangeSupport(PropertyChangeSupport value)
    {
        _propertyChangeSupport = value;
    }
    
    /**
     * Sets the type of the node. Possible types: FOLDER and ITEM
     * 
     * @param value the new type of the node.
     */
    public void setType(int value)
    {
        _type = value;
    }
    
}
