package com.recomdata.transmart.data.export.omicsoftIntegration

import org.codehaus.groovy.grails.commons.GrailsApplication

/**
 * Created with IntelliJ IDEA.
 * User: transmart
 * Date: 12/20/13
 * Time: 2:13 AM
 * To change this template use File | Settings | File Templates.
 */
class OmicsoftExportService {

	// consts (start)
	final String zipExt = ".zip"
	// consts (end)

	// services (start)
	GrailsApplication grailsApplication
	// services (end)

	/**
	 *
	 * @param jobName
	 * @return file from temp directory
	 */
	File getOmicsoftFileByJobName(jobName, zip) {
	//	def tmpDir = grailsApplication.config.com.recomdata.plugins.tempFolderDirectory
        def tmpDir =  grailsApplication.config.com.recomdata.plugins.tempFolderDirectory
        def filePath
        if (zip) {
            filePath = tmpDir + File.separator + jobName + zipExt
            return new File(filePath)
        } else {
            //find osprj file in folder
            def fileFolder = new File(tmpDir + File.separator + jobName)
            filePath = null
            fileFolder.eachFileMatch (~/.*\.osprj/) { file ->
                filePath = file
            }
            return filePath;
        }
    }

}
