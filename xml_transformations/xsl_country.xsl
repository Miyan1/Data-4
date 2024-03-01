<?xml version="1.0" encoding="UTF-8" ?>
<xsl:stylesheet
        version="2.0"
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="text"/>
    <xsl:template match="/">
        <xsl:for-each select="/CountryList/country">
            <xsl:text>INSERT INTO country2 (code, code3, name) VALUES </xsl:text>
            <xsl:text>('</xsl:text>
            <xsl:value-of select="./@sc"/>
            <xsl:text>', '</xsl:text>
            <xsl:value-of select="./@lc"/>
            <xsl:text>', '</xsl:text>
            <xsl:value-of select="./@co_name"/>
            <xsl:text>');</xsl:text>
            <xsl:text>&#xa;</xsl:text>
        </xsl:for-each>
    </xsl:template>
</xsl:stylesheet>
