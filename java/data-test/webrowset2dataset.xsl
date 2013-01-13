<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xs="http://www.w3.org/2001/XMLSchema" exclude-result-prefixes="xs">
	<xsl:output method="xml" encoding="UTF-8" indent="yes"/>
	<xsl:template match="/">
		<dataset>
			<xsl:for-each select="webRowSet">
				<xsl:for-each select="metadata">
					<metadata>
						<xsl:for-each select="column-definition">
							<col>
								<xsl:for-each select="column-type">
									<xsl:attribute name="type">
										<xsl:value-of select="string(floor(number(string(.))))"/>
									</xsl:attribute>
								</xsl:for-each>
								<xsl:for-each select="nullable">
									<xsl:attribute name="nullable">
										<xsl:value-of select="string(boolean(floor(number(string(.)))))"/>
									</xsl:attribute>
								</xsl:for-each>
								<xsl:for-each select="column-type-name">
									<xsl:attribute name="native_type">
										<xsl:value-of select="string(.)"/>
									</xsl:attribute>
								</xsl:for-each>
								<xsl:for-each select="column-name">
									<xsl:attribute name="name">
										<xsl:value-of select="string(.)"/>
									</xsl:attribute>
								</xsl:for-each>
							</col>
						</xsl:for-each>
					</metadata>
				</xsl:for-each>
				<xsl:for-each select="data">
					<data>
						<xsl:for-each select="currentRow">
							<row>
								<xsl:for-each select="columnValue">
									<value>
										<xsl:value-of select="string(.)"/>
									</value>
								</xsl:for-each>
							</row>
						</xsl:for-each>
					</data>
				</xsl:for-each>
			</xsl:for-each>
		</dataset>
	</xsl:template>
</xsl:stylesheet>
