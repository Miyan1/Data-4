<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0"
                xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:xs="http://www.w3.org/2001/XMLSchema">

    <xsl:output method="text" encoding="UTF-8"/>

    <xsl:template name="processCityName">
        <xsl:param name="text"/>
        <xsl:if test="string-length($text) > 0">
            <xsl:variable name="textWithoutBrackets" select="translate($text, '&lt;&gt;', '')"/>
            <xsl:choose>
                <xsl:when test="starts-with($textWithoutBrackets, &quot;'&quot;)">
                    <xsl:text>''</xsl:text>
                    <xsl:call-template name="processCityName">
                        <xsl:with-param name="text" select="substring($textWithoutBrackets, 2)"/>
                    </xsl:call-template>
                </xsl:when>
                <xsl:otherwise>
                    <xsl:value-of select="substring($textWithoutBrackets, 1, 1)"/>
                    <xsl:call-template name="processCityName">
                        <xsl:with-param name="text" select="substring($textWithoutBrackets, 2)"/>
                    </xsl:call-template>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:template>

    <xsl:template match="/">
        <xsl:for-each select="/CountryList/country/city">
            <xsl:text>INSERT INTO city2 (city_id, city_name, latitude, longitude, postal_code, country_code) VALUES (CAST('' AS XML).value('xs:base64Binary("</xsl:text>
            <xsl:value-of select="translate(./@city_id, '&quot;', '')"/>
            <xsl:text>")', 'VARBINARY(MAX)'), '</xsl:text>
            <xsl:call-template name="processCityName">
                <xsl:with-param name="text" select="@ci_name"/>
            </xsl:call-template>
            <xsl:text>', '</xsl:text>
            <xsl:value-of select="./geo/lat"/>
            <xsl:text>', '</xsl:text>
            <xsl:value-of select="./geo/long"/>
            <xsl:text>', '</xsl:text>
            <xsl:value-of select="./@post"/>
            <xsl:text>', '</xsl:text>
            <xsl:value-of select="../@sc"/>
            <xsl:text>');</xsl:text>
            <xsl:text>&#xa;</xsl:text>
        </xsl:for-each>
    </xsl:template>
</xsl:stylesheet>













