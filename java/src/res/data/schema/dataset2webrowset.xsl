<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema" exclude-result-prefixes="xs">
	<xsl:output method="xml" encoding="UTF-8" indent="yes"/>
	<xsl:template match="/">
		<webRowSet>
			<xsl:for-each select="dataset">
				<properties>
					<xsl:for-each select="name">
						<table-name>
							<xsl:value-of select="string(.)"/>
						</table-name>
					</xsl:for-each>
				</properties>
				<xsl:for-each select="metadata">
					<xsl:variable name="var1_col" select="col"/>
					<metadata>
						<column-count>
							<xsl:value-of select="string(count($var1_col))"/>
						</column-count>
						<xsl:for-each select="$var1_col">
							<xsl:variable name="var2_resultof_cast" select="string(@nullable)"/>
							<column-definition>
								<column-index>
									<xsl:value-of select="string(position())"/>
								</column-index>
								<nullable>
									<xsl:value-of select="string(floor(number(((normalize-space($var2_resultof_cast) = 'true') or (normalize-space($var2_resultof_cast) = '1')))))"/>
								</nullable>
								<column-name>
									<xsl:value-of select="string(@name)"/>
								</column-name>
								<column-type>
									<xsl:value-of select="string(floor(number(string(@type))))"/>
								</column-type>
								<column-type-name>
									<xsl:value-of select="string(@native_type)"/>
								</column-type-name>
							</column-definition>
						</xsl:for-each>
					</metadata>
				</xsl:for-each>
				<xsl:for-each select="data">
					<data>
						<xsl:for-each select="row">
							<currentRow>
								<xsl:for-each select="value">
									<columnValue>
										<xsl:value-of select="string(.)"/>
									</columnValue>
								</xsl:for-each>
							</currentRow>
						</xsl:for-each>
					</data>
				</xsl:for-each>
			</xsl:for-each>
		</webRowSet>
	</xsl:template>
</xsl:stylesheet>
