/*
 * XmlUtil.java
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.toolsverse.util;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.EntityResolver;
import org.xml.sax.InputSource;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;

/**
 * XmlUtil - A wrapper to simplify basic XML functions.
 * 
 * The goal is to provide an easy and clear set of routines for building,
 * accessing, and moifying XML trees from a variety of sources (file, stream,
 * etc). Sun's XML implimentation is very complex and provides a large set of
 * features that are not really necessary in many applications. XmlUtil helps to
 * narrow it down into a subset of basic, intuitive methods, hence stream-
 * lining application development involving XML.
 * 
 */

public final class XmlUtils
{
    
    protected Document masterXmlDoc;
    
    /**
     * Constructs an XmlUtil object from an XML file object.
     * 
     * @param fileObject
     *            An object of a valid XMl file. The file object's path will be
     *            made absolute to ensure sanity.
     * 
     */
    public XmlUtils(File fileObject) throws Exception
    {
        this(fileObject, null);
    }
    
    /**
     * Constructs an XmlUtil object from an XML file object.
     * 
     * @param fileObject
     *            An object of a valid XMl file. The file object's path will be
     *            made absolute to ensure sanity.
     * 
     */
    public XmlUtils(File fileObject, EntityResolver er) throws Exception
    {
        DocumentBuilder docBuilder = makeDoc();
        if (er != null)
            docBuilder.setEntityResolver(er);
        
        masterXmlDoc = docBuilder.parse(fileObject);
        masterXmlDoc.getDocumentElement().normalize();
    }
    
    /**
     * Constructs an XmlUtil object to provide easy wrapping for XML operations.
     * Creates a tree from XML structured data in stream form.
     * 
     * @param inStream
     *            A data stream that is parsed by the docBuilder for better
     *            performance over file objects. Requested by Michael "Wang"
     *            Mandel.
     * 
     */
    public XmlUtils(InputStream inStream) throws Exception
    {
        masterXmlDoc = makeDoc().parse(inStream);
        masterXmlDoc.getDocumentElement().normalize();
    }
    
    public XmlUtils(String strtoParse) throws Exception
    {
        StringReader strReader = new StringReader(strtoParse);
        InputSource iSource = new InputSource(strReader);
        masterXmlDoc = makeDoc().parse(iSource);
        masterXmlDoc.getDocumentElement().normalize();
    }
    
    /**
     * Creates an empty tree with a root node of the specified name and value (a
     * null can be substituted for value if none is desired - this is often
     * preferrable).
     * 
     * @param rootName
     *            The name you want to give to the tree's root node.
     * @param rootValue
     *            A value, if any, that you want the #text field of the root
     *            node to contain. May be null.
     * 
     */
    public XmlUtils(String rootName, String rootValue) throws Exception
    {
        // Setup the builder
        DocumentBuilder builder = makeDoc();
        
        masterXmlDoc = builder.newDocument();
        
        Element root = masterXmlDoc.createElement(rootName);
        
        masterXmlDoc.appendChild(root);
        
    }
    
    /**
     * Adds an attribute to a specified Node.
     * 
     * @param node
     *            A pre-existing Node on the XML tree under which the new Node
     *            should be placed as a child. The node must implement the
     *            Element interface, or a ClassCastException will occur.
     * @param attributeName
     *            The name of the attribute to be created.
     * @param attributeValue
     *            The value of the attribute to be created.
     * 
     * @return The memory address of the newly created attribute.
     * 
     */
    
    public Attr addAttribute(Node node, String attributeName,
            String attributeValue)
    {
        Element element = (Element)node;
        Attr attrib = masterXmlDoc.createAttribute(attributeName);
        attrib.setValue(attributeValue);
        
        element.setAttributeNode(attrib);
        
        return attrib;
        
    }
    
    /**
     * Adds a new Node beneath a specified parent Node, but does NOT assign it a
     * value (the value is set to null).
     * 
     * @param parentNode
     *            A pre-existing Node on the XML tree under which the new Node
     *            should be placed as a child. Not tested for specification of a
     *            Node not already on the tree.
     * @param fieldName
     *            The name of the field to be created.
     * 
     * @return The memory address of the newly created Node.
     * 
     */
    public Node addFieldUnder(Node parentNode, String fieldName)
    {
        Element newField = masterXmlDoc.createElement(fieldName);
        parentNode.appendChild(newField);
        newField.appendChild(masterXmlDoc.createTextNode(null));
        
        return newField;
    }
    
    /**
     * Adds a new Node beneath a specified parent Node.
     * 
     * @param parentNode
     *            A pre-existing Node on the XML tree under which the new Node
     *            should be placed as a child. Not tested for specification of a
     *            Node not already on the tree.
     * @param fieldName
     *            The name of the field to be created.
     * @param fieldValue
     *            What should be placed in the #text child of the new field.
     * 
     * @return The memory address of the newly created Node.
     * 
     */
    public Node addFieldUnder(Node parentNode, String fieldName,
            String fieldValue)
    {
        Element newField = masterXmlDoc.createElement(fieldName);
        parentNode.appendChild(newField);
        newField.appendChild(masterXmlDoc.createTextNode(fieldValue));
        
        return newField;
    }
    
    /**
     * Adds a new Node directly beneath the XML tree's root. This eliminates the
     * need to pass the actual root node into either of the addFieldUnder
     * methods.
     * 
     * @param fieldName
     *            The name of the field to be created.
     * 
     */
    public Node addRootField(String fieldName)
    {
        return addFieldUnder(masterXmlDoc.getFirstChild(), fieldName);
    }
    
    /**
     * Adds a new Node directly beneath the XML tree's root. This eliminates the
     * need to pass the actual root node into either of the addFieldUnder
     * methods.
     * 
     * @param fieldName
     *            The name of the field to be created.
     * @param fieldValue
     *            What should be placed in the #text child of the new field.
     * 
     */
    public Node addRootField(String fieldName, String fieldValue)
    {
        return addFieldUnder(masterXmlDoc.getFirstChild(), fieldName,
                fieldValue);
    }
    
    /**
     * Allows updates to the first child node of a field (called "#text" that is
     * the actual location of a field's value). Streamlines this process.
     * 
     * @param aNode
     *            The actual Node object that you want to update.
     * @param newValue
     *            A String to replace the current #text field with.
     * 
     */
    public void changeFieldValue(Node aNode, String newValue)
    {
        aNode.getFirstChild().setNodeValue(newValue);
    }
    
    /**
     * Allows updates to the first child node of a field (called "#text" that is
     * the actual location of a field's value). Streamlines this process.
     * 
     * @param nodeName
     *            A String for the name of a node. Searches the XML tree for the
     *            first Node of this name.
     * @param newValue
     *            A String to replace the current #text field with.
     * 
     */
    public void changeFieldValue(String nodeName, String newValue)
    {
        Node tempNode = getFirstNodeNamed(nodeName);
        
        tempNode.getFirstChild().setNodeValue(newValue);
    }
    
    /**
     * Deletes passed in Node and all of that Node's children.
     * 
     * @param aNode
     *            The Node to remove.
     * 
     */
    public void deleteNode(Node aNode)
    {
        aNode.getParentNode().removeChild(aNode);
    }
    
    /**
     * Deletes all the Nodes contained in a Node Vector.
     * 
     * @param manyNodes
     *            A Vector of Nodes to remove.
     * 
     * @return Returns the number of Nodes deleted.
     * 
     */
    public int deleteNodesIn(Vector<Node> manyNodes)
    {
        int limit = manyNodes.size();
        for (int i = 0; i < limit; i++)
            deleteNode(manyNodes.get(i));
        return limit;
    }
    
    /**
     * Deletes all the Nodes in an XML tree named nodeName.
     * 
     * @param nodeName
     *            Search name of Nodes to be deleted.
     * 
     * @return Reutnrs the number of Nodes deleted.
     * 
     */
    public int deleteNodesNamed(String nodeName)
    {
        return deleteNodesIn(getNodesNamed(nodeName));
    }
    
    /**
     * Dumps the contents of the XML tree in raw form to the passed in
     * OutputStream.
     * 
     * @param outStream
     *            Stream object to write XmlTree to.
     * 
     */
    @SuppressWarnings("deprecation")
    public void dumpTo(OutputStream outStream)
        throws IOException
    {
        org.apache.xml.serialize.OutputFormat out = new org.apache.xml.serialize.OutputFormat(
                masterXmlDoc);
        out.setIndenting(true);
        out.setLineWidth(3000);
        org.apache.xml.serialize.XMLSerializer xml = new org.apache.xml.serialize.XMLSerializer(
                outStream, out);
        xml.serialize(masterXmlDoc);
    }
    
    /**
     * Dumps the contents of the XML tree in raw form to the passed in
     * OutputStream.
     * 
     * @param outStream
     *            Stream object to write XmlTree to.
     * 
     */
    @SuppressWarnings("deprecation")
    public void dumpTo(Writer outStream)
        throws IOException
    {
        org.apache.xml.serialize.OutputFormat out = new org.apache.xml.serialize.OutputFormat(
                masterXmlDoc);
        out.setIndenting(true);
        out.setLineWidth(3000);
        org.apache.xml.serialize.XMLSerializer xml = new org.apache.xml.serialize.XMLSerializer(
                outStream, out);
        xml.serialize(masterXmlDoc);
    }
    
    public String format(int indent, int lineWidth)
        throws IOException
    {
        OutputFormat format = new OutputFormat();
        format.setLineWidth(lineWidth);
        format.setIndenting(true);
        format.setIndent(indent);
        
        StringWriter stringOut = new StringWriter();
        XMLSerializer serial = new XMLSerializer(stringOut, format);
        serial.serialize(masterXmlDoc);
        
        return stringOut.toString();
    }
    
    public Node getAttribute(Node node, String attrName)
    {
        NamedNodeMap attrs = node.getAttributes();
        
        if (attrs == null || attrs.getLength() <= 0)
            return null;
        
        Node attrNode = attrs.getNamedItem(attrName);
        
        return attrNode;
    }
    
    public Map<String, String> getAttributes(Node node)
    {
        NamedNodeMap map = node.getAttributes();
        
        if (map == null || map.getLength() == 0)
            return null;
        
        Map<String, String> result = new HashMap<String, String>();
        
        for (int i = 0; i < map.getLength(); i++)
        {
            Node attrNode = map.item(i);
            
            if ((attrNode.getNodeType() == Node.TEXT_NODE)
                    || (attrNode.getNodeType() == Node.COMMENT_NODE))
                continue;
            
            result.put(attrNode.getNodeName(), attrNode.getNodeValue());
        }
        
        return result;
    }
    
    public Map<String, String> getAttributes(Node parent, String name)
    {
        Node node = getFirstNodeNamed(parent, name);
        
        if (node == null)
            return null;
        
        return getAttributes(node);
    }
    
    public Boolean getBooleanAttribute(Node node, String attrName)
    {
        Node attrNode = getAttribute(node, attrName);
        
        if (attrNode == null)
            return null;
        
        return Utils.str2Boolean(attrNode.getNodeValue(), null);
    }
    
    /**
     * Gets a Vector of Nodes of all the children of a particular Node. Superior
     * for uses with XmlUtil insofar as it returns ONLY child Nodes of rootNode,
     * rather than Nodes *and* child #text fields.
     * 
     * @param rootNode
     *            The parent Node from which we'll get the child Nodes.
     * 
     */
    public Vector<Node> getChildNodes(Node rootNode)
    {
        NodeList nodeList = rootNode.getChildNodes();
        Vector<Node> nodeVector = new Vector<Node>();
        int nodeCount = nodeList.getLength();
        
        for (int i = 0; i < nodeCount; i++)
        {
            if (nodeList.item(i).getNodeName() != "#text")
            {
                nodeVector.add(nodeList.item(i));
            }
        }
        
        return nodeVector;
    }
    
    /**
     * Gets a Vector of Strings of all the values contained by the children of a
     * particular Node.
     * 
     * @param rootNode
     *            The parent Node from which we'll get the values of the
     *            children.
     * 
     */
    public Vector<String> getChildValues(Node rootNode)
    {
        Vector<String> stringVector = new Vector<String>();
        Vector<Node> nodeVector = getChildNodes(rootNode);
        
        int nodeCount = nodeVector.size();
        
        for (int i = 0; i < nodeCount; i++)
            stringVector.add(getValueOf(nodeVector.get(i)));
        
        return stringVector;
    }
    
    public Document getDoc()
    {
        return masterXmlDoc;
    }
    
    /**
     * Searches the XML tree for a field that contains searchString and returns
     * the address of that Node.
     * 
     * @param searchString
     *            the value to search for.
     * 
     * @return The address of the Node containing searchString. If it was not
     *          found, the method returns null.
     * 
     */
    public Node getFirstNodeContaining(String searchString)
    {
        NodeList nodeList = masterXmlDoc.getElementsByTagName("*");
        String nodeValue = null;
        int listSize = nodeList.getLength();
        
        for (int i = 0; i < listSize; i++)
        {
            nodeValue = (nodeList.item(i) != null && nodeList.item(i)
                    .getNodeValue() != null) ? nodeList.item(i).getNodeValue()
                    : "";
            if (nodeValue.equalsIgnoreCase(searchString))
            {
                return nodeList.item(i);
            }
            NamedNodeMap attrs = (nodeList.item(i) != null) ? nodeList.item(i)
                    .getAttributes() : null;
            int mapSize = attrs != null ? attrs.getLength() : 0;
            String attrName = null;
            String attrValue = null;
            for (int j = 0; j < mapSize; j++)
            {
                attrName = (attrs.item(j) != null) ? attrs.item(j)
                        .getNodeName() : "";
                attrValue = (attrs.item(j) != null) ? attrs.item(j)
                        .getNodeValue() : "";
                if (attrName.equalsIgnoreCase(searchString))
                    return nodeList.item(i);
                if (attrValue.equalsIgnoreCase(searchString))
                    return nodeList.item(i);
            }
        }
        
        return null;
    }
    
    /**
     * Searches ONLY THE FIRST LEVEL CHILDREN of specified rootNode and returns
     * the first instance of a Node named nodeName. This routine does NOT
     * transverse multiple levels (this is intentional). It is meant for mainly
     * for getting data from reoccurring packetized blocks of fields.
     * 
     * @param rootNode
     *            The Node whose children we'll search for a Node called
     *            nodeName.
     * @param nodeName
     *            The name of the Node we're looking for.
     * 
     * @return The Node we're looking for, or null if it does not exist. (Make
     *         sure that you check for null condition properly when using this.
     *         This routine is used frequently in cases where we're not sure
     *         what the child Nodes of rootNode are. The return value will have
     *         no properties if no match is found.)
     * 
     */
    public Node getFirstNodeNamed(Node rootNode, String nodeName)
    {
        NodeList nodeList = rootNode.getChildNodes();
        Node tempNode = null;
        int listSize = nodeList.getLength();
        
        for (int i = 0; i < listSize; i++)
        {
            tempNode = nodeList.item(i);
            // if ((tempNode.getNodeName() == nodeName) ||
            // (tempNode.getNodeName() + " " == nodeName)) {
            if (nodeName.equalsIgnoreCase(tempNode.getNodeName().trim()))
            {
                return tempNode;
            }
        }
        
        return null;
    }
    
    /**
     * Searches the entire XML tree for a Node of specified String searchString
     * and returns the address of that Node.
     * 
     * @param searchString
     *            A field to search for.
     * 
     * @return The address of the Node named searchString. If it was not found,
     *         the method returns null.
     * 
     */
    public Node getFirstNodeNamed(String searchString)
    {
        NodeList tempList = masterXmlDoc.getElementsByTagName(searchString
                .trim());
        if (tempList.item(0) != null)
            return tempList.item(0);
        
        return null;
    }
    
    /**
     * Searches the XML tree for Nodes contaning String searchString and returns
     * the addresses of those Nodes in a Vector.
     * 
     * @param searchString
     *            A field value to search for.
     * 
     * @return A list of all nodes containing searchString.
     * 
     */
    public Vector<Node> getNodesContaining(String searchString)
    {
        Vector<Node> nodeVector = new Vector<Node>();
        nodeVector.clear();
        NodeList tempList = masterXmlDoc.getElementsByTagName("*");
        String tempString = null;
        int listSize = tempList.getLength();
        
        for (int i = 0; i < listSize; i++)
        {
            tempString = tempList.item(i).getFirstChild().getNodeValue().trim();
            if (tempString.equalsIgnoreCase(searchString))
            {
                nodeVector.add(tempList.item(i));
            }
        }
        
        return nodeVector;
    }
    
    /**
     * Searches the XML tree for Nodes named String searchString and returns
     * their addresses in a vector.
     * 
     * @param searchString
     *            A field name to search for.
     * 
     * @return A list of all nodes matching the name searchString.
     * 
     */
    public synchronized Vector<Node> getNodesNamed(String searchString)
    {
        Vector<Node> nodeVector = new Vector<Node>();
        nodeVector.clear();
        NodeList nodeList = masterXmlDoc.getElementsByTagName(searchString);
        int listSize = nodeList.getLength();
        for (int i = 0; i < listSize; i++)
            nodeVector.add(nodeList.item(i));
        return nodeVector;
    }
    
    public String getNodeValue(Node rootNode, String name)
    {
        Node currentNode = getFirstNodeNamed(rootNode, name);
        
        if (currentNode == null)
            return null;
        
        return getValueOf(currentNode);
    }
    
    public Number getNumberAttribute(Node node, String attrName)
    {
        Node attrNode = getAttribute(node, attrName);
        
        if (attrNode == null)
            return null;
        
        return Utils.str2Number(attrNode.getNodeValue(), null);
    }
    
    /**
     * Returns a live instance of the tree's root node that children can be
     * accessed, modified, added, and deleted from. Useful for adding
     * first-level tree branches.
     * 
     * @return A live instance of the tree's root node.
     * 
     */
    public Node getRootNode()
    {
        return masterXmlDoc.getDocumentElement();
    }
    
    public String getStringAttribute(Node node, String attrName)
    {
        Node attrNode = getAttribute(node, attrName);
        
        if (attrNode == null)
        {
            return null;
        }
        
        String result = attrNode.getNodeValue();
        
        return result;
    }
    
    /**
     * Looks at the first child node of aNode for the value of the field.
     * 
     * @param aNode
     *            A Node object to fetch the value from.
     * 
     * @return The value stored in the field.
     * 
     */
    public String getValueOf(Node aNode)
    {
        if (aNode.hasChildNodes())
        {
            return aNode.getFirstChild().getNodeValue();
        }
        else
        {
            return null;
        }
    }
    
    /**
     * Searches the XML tree for a node called NodeName and then returns a
     * String of it's value (the first child of the Node). Gets the first
     * instance of the Node.
     * 
     * @param nodeName
     *            String of the node to search for.
     * 
     * @return The value stored in the field, null if nodeName was not found.
     * 
     */
    public String getValueOf(String nodeName)
    {
        return getValueOf(getFirstNodeNamed(nodeName));
    }
    
    /**
     * A couple common routines for building an XML Document. This should NEVER
     * be called outside of XmlUtil. (And of course you can't, so don't make it
     * public you naughty coders who are now reading my implimentations.)
     * 
     */
    private DocumentBuilder makeDoc()
        throws Exception
    {
        DocumentBuilder docBuilder = null;
        // Setup the builder
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
                "org.apache.xerces.jaxp.DocumentBuilderFactoryImpl");
        DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
                .newInstance();
        
        docBuilder = docBuilderFactory.newDocumentBuilder();
        
        return docBuilder;
    }
    
    public void setDoc(Document d)
    {
        masterXmlDoc = d;
    }
    
    /**
     * Dumps the contents of the XML tree into an XML formatted file as
     * specified by the File fileObject.
     * 
     * @param fileObject
     *            The pre-created File object to dump the tree into.
     * 
     */
    public void writeToFile(File fileObject)
        throws IOException
    {
        FileWriter tempFileWriter = null;
        
        try
        {
            tempFileWriter = new FileWriter(fileObject);
            dumpTo(tempFileWriter);
        }
        finally
        {
            if (tempFileWriter != null)
            {
                tempFileWriter.flush();
                tempFileWriter.close();
            }
        }
    }
    
    /**
     * Dumps the contents of the XML tree into an XML formatted file specified
     * in the path String fileNameString.
     * 
     * @param fileName
     *            The filename to dump the tree into.
     * 
     */
    public void writeToFile(String fileName)
        throws IOException
    {
        writeToFile(new File(fileName));
    }
    
    /**
     * Dumps the contents of the XML tree in raw form to the String
     * 
     * 
     */
    public String xmlToString()
        throws IOException
    {
        ByteArrayOutputStream outS = null;
        
        try
        {
            outS = new ByteArrayOutputStream();
            
            dumpTo(outS);
            
            return outS.toString();
        }
        finally
        {
            if (outS != null)
                outS.close();
        }
    }
    
}
