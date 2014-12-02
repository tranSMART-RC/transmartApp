/*************************************************************************
 * tranSMART - translational medicine data mart
 * 
 * Copyright 2008-2012 Janssen Research & Development, LLC.
 * 
 * This product includes software developed at Janssen Research & Development, LLC.
 * 
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License 
 * as published by the Free Software  * Foundation, either version 3 of the License, or (at your option) any later version, along with the following terms:
 * 1.	You may convey a work based on this program in accordance with section 5, provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it, in any medium, provided that you retain the above notices.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS    * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 *
 ******************************************************************/
  

package com.recomdata.transmart.data.export

import com.recomdata.transmart.data.export.omicsoftIntegration.OmicsoftExportService
import com.recomdata.transmart.data.export.omicsoftIntegration.OmicsoftProjectDataService
import org.apache.commons.io.FilenameUtils
import org.codehaus.groovy.grails.commons.ConfigurationHolder
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.json.JSONObject

import javax.servlet.http.HttpSession

class OmicsoftExportController {

    //def index = { }


	// services (start)
	def exportService
	def springSecurityService
    def omicsoftExpressionDataService
	def omicsoftClinicalDataService
    OmicsoftProjectDataService omicsoftProjectDataService
	def config = ConfigurationHolder.config
	GrailsApplication grailsApplication
	OmicsoftExportService omicsoftExportService
	// services (end)
	
	//We need to gather a JSON Object to represent the different data types.
	def getMetaData =
	{
		response.setContentType("text/json")
		render exportService.getMetaData(params)
	}

	def downloadFileExists = {
		def InputStream inputStream = exportService.downloadFile(params);
		response.setContentType("text/json")
		JSONObject result = new JSONObject()
		
		if(null != inputStream){
			result.put("fileStatus", true)
		} else {
		   	result.put("fileStatus", false)
			result.put("message", "Download failed as file could not be found on the server")
	    }
		response.outputStream << result.toString()
	}
	
	def downloadFile = {
		def InputStream inputStream = exportService.downloadFile(params);
		
		def fileName = params.jobname + ".zip"
		response.setContentType "application/octet-stream"
		response.setHeader "Content-disposition", "attachment;filename=${fileName}"
		response.outputStream << inputStream
		response.outputStream.flush()
		inputStream.close();
		return true;
	}
	
	/**
	* Method that will create the new asynchronous job name
	* Current methodology is username-jobtype-ID from sequence generator
	*/
   def createnewjob = {
	   def result = exportService.createExportDataAsyncJob(params, springSecurityService.getPrincipal().username)
	   
	   response.setContentType("text/json")
	   response.outputStream << result.toString()
   }
	
	
	/**
	* Method that will run a data export and is called asynchronously from the datasetexplorer -> Data Export tab
	*/
    def runDataExport = 	{
		def jsonResult = exportService.exportData(params, springSecurityService.getPrincipal().username)
		
		response.setContentType("text/json")
		response.outputStream << jsonResult.toString()
    }

    def doExport = {
        String exportType = request.getParameter("export_type");
        String resultInstanceId = request.getParameter("result_instance_id");
        String subset = request.getParameter("subset_id");

		response.setContentType("text/json")


        if (session.getAttribute("finishedOmicsoftResultInstances") == null) {
            session.setAttribute("finishedOmicsoftResultInstances", Collections.synchronizedMap(new HashMap<String, String>()))
        }

        session.getAttribute("finishedOmicsoftResultInstances").put(resultInstanceId, null);

        if (resultInstanceId != null && !resultInstanceId.isEmpty()
                && exportType != null && !exportType.isEmpty()) {
            final HttpSession innerSession = session
            final innerLog = log
            new Thread(new Runnable() {
                @Override
                void run() {
                    try {
                        String jobName = "undefined"
                        if (exportType.equals("1")) {      // Clinical
                            jobName = omicsoftClinicalDataService.getClinicalDataByResultInstanceId(resultInstanceId, subset)
                        } else if (exportType.equals("2")) {       //Biomarker/expression
                            jobName = omicsoftExpressionDataService.exportExpressionData(resultInstanceId, subset);
                        } else if (exportType.equals("4") || exportType.equals("5")) {       // Omicsoft project
                            jobName = omicsoftProjectDataService.getProjectDataByResultInstanceId(resultInstanceId, subset);
                        } else if (exportType.equals("3")) {       // SNP

                        }
                        innerSession.getAttribute("finishedOmicsoftResultInstances").put(resultInstanceId, jobName)
                    } catch (Exception e) {
                        innerSession.getAttribute("finishedOmicsoftResultInstances").put(resultInstanceId, "fail")
                        innerLog.error("Error exporting data", e)
                    }
                }
            }).start()

            render 'success';
        } else {
            render 'error';
        }
    }


    def checkOmicsoftProjectExport = {
        response.setContentType("text/json")
        String resultInstanceId = request.getParameter("result_instance_id")
        if (session.getAttribute("finishedOmicsoftResultInstances").get(resultInstanceId) != null) {
            render session.getAttribute("finishedOmicsoftResultInstances").get(resultInstanceId)
        } else {
            render 'pending'
        }
    }

	def sendFileToResposce(
	    response,
		String filename
	) {
		BufferedReader br
		try {
			response.setContentType("application/octet-stream");
			def baseFilename = FilenameUtils.getName(filename)
			response.setHeader("Content-disposition", "attachment;filename=" + baseFilename);

			br = new BufferedReader(new FileReader(filename))
			String line;
			while ( (line = br.readLine()) != null ) {
				response.outputStream << line << "\n"
			}
			response.outputStream.flush()
			br.close()
		} catch (Exception e) {
			e.printStackTrace()
			throw e
		} finally {
			response.outputStream.flush()
			br.close()
		}
	} // def sendFileToResponse


	def downloadOmicsoftFile = {
        def jobName = params.filename
        def jobType = params.jobType

        //5 - OmicSoft project, not zip
        File requestedFile = omicsoftExportService.getOmicsoftFileByJobName(jobName, !"5".equals(jobType)/*zip*/)
        def filename = requestedFile.getName()

        response.setContentType("application/octet-stream");
        response.setHeader("Content-disposition", "attachment;filename=" + filename)
        response.outputStream << requestedFile.newInputStream()
		response.outputStream.flush()
	}

}



