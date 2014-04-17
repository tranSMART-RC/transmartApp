package com.thomsonreuters.lsps.transmart

import groovyx.net.http.*
import static groovyx.net.http.ContentType.*
import static groovyx.net.http.Method.*
import org.apache.http.auth.*

class HttpBuilderService {
	boolean transactional = true
	
	def grailsApplication
	
	def getInstance(uri) {
		def site = new HTTPBuilder(uri)
		if (grailsApplication.config.com.thomsonreuters.transmart.proxyHost && grailsApplication.config.com.thomsonreuters.transmart.proxyPort) {
			println ("proxy: "+grailsApplication.config.com.thomsonreuters.transmart.proxyHost+" "+grailsApplication.config.com.thomsonreuters.transmart.proxyPort)
			log.info "Using proxy -> ${grailsApplication.config.com.thomsonreuters.transmart.proxyHost}:${grailsApplication.config.com.thomsonreuters.transmart.proxyPort}"
			if (grailsApplication.config.com.thomsonreuters.transmart.proxyUser) {
				log.info "Authenticating with proxy as ${grailsApplication.config.com.thomsonreuters.transmart.proxyUser}"
				if (grailsApplication.config.com.thomsonreuters.transmart.proxyNTLMDomain) log.info "NTLM domain: ${grailsApplication.config.com.thomsonreuters.transmart.proxyNTLMDomain}"
				site.client.getCredentialsProvider().setCredentials(
				    new AuthScope(grailsApplication.config.com.thomsonreuters.transmart.proxyHost, grailsApplication.config.com.thomsonreuters.transmart.proxyPort.toInteger()),
				    new NTCredentials(grailsApplication.config.com.thomsonreuters.transmart.proxyUser, grailsApplication.config.com.thomsonreuters.transmart.proxyPassword, 
						InetAddress.getLocalHost().getHostName(), grailsApplication.config.com.thomsonreuters.transmart.proxyDomain)
				)
			}
				
			site.setProxy(grailsApplication.config.com.thomsonreuters.transmart.proxyHost, grailsApplication.config.com.thomsonreuters.transmart.proxyPort.toInteger(), null)
		}
		
		return site
		
	}
	
}