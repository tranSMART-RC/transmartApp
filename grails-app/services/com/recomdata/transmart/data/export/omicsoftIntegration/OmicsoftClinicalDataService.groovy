package com.recomdata.transmart.data.export.omicsoftIntegration

import com.recomdata.dataexport.util.ExportUtil
import com.recomdata.snp.SnpData
import com.recomdata.transmart.data.export.ClinicalDataService
import com.recomdata.transmart.data.export.DataExportService
import com.recomdata.transmart.data.export.I2b2ExportHelperService
import com.recomdata.transmart.data.export.exception.DataNotFoundException
import com.recomdata.transmart.data.export.omicsoftIntegration.util.SqlUtilsService
import com.recomdata.transmart.data.export.util.FileWriterUtil
import com.recomdata.transmart.util.UtilService
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.codehaus.groovy.grails.commons.ConfigurationHolder
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.rosuda.REngine.REXP
import org.rosuda.REngine.Rserve.RConnection
import org.springframework.transaction.annotation.Transactional

import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

/**
 * Created with IntelliJ IDEA.
 * User: transmart
 * Date: 12/16/13
 * Time: 1:45 AM
 * To change this template use File | Settings | File Templates.
 */
class OmicsoftClinicalDataService extends DataExportService {

	// consts (start)
	final String typeNameMrna = 'mrna'
	final String typeNameClinical = 'clinical'
	final String subset1 = 'subset1'
	final String subset2 = 'subset2'
	final String clinicalDefaultFilename = 'clinical_i2b2trans' // DO NOT CHANGE!!! HARDCOD UNDER.
	final String clinicalExportJobName = 'omicsoftClinicalExport'
	final String convertedClinicalDataDefaultFilename = 'clinical.design.txt'
	// consts (end)

	// services (start)
	def clinicalDataService
	def i2b2ExportHelperService
	def grailsApplication
	def utilService
	def i2b2HelperService
	def config = ConfigurationHolder.config
	def sqlUtilsService
	//DataConvertors dataConvertors
	// services (end)

	/**
	 *
	 * @param _resultInstanceId
	 * @param _subset
	 * @return zip filename
	 * @throws Exception
	 */
	@Transactional(readOnly = true)
	String getClinicalDataByResultInstanceId(
		String _resultInstanceId,
		String _subset
	) throws Exception {
		String resultInstanceId = pullOutValue(_resultInstanceId)
		String subset = specifySubset( pullOutValue(_subset) )
		// managing temp directories
		//def tmpDir = grailsApplication.config.com.recomdata.transmart.data.export.jobTmpDirectory
	//	def tmpDir = grailsApplication.config.com.recomdata.plugins.tempFolderDirectory
        def tmpDir =  grailsApplication.config.com.recomdata.plugins.tempFolderDirectory
		def jobName = sqlUtilsService.generateOmicsoftJobName() //'omicsoftExport_' + (new Date().format('yyyy_MM_dd__HH_mm_ss'))
		def targetFolder = tmpDir + File.separator + jobName + File.separator
		createDir(targetFolder)

		// get study names for provided result_instance_id
		def studyList = i2b2ExportHelperService.findStudyAccessions( [resultInstanceId])

		if ( studyList.size() == 0 )
			throw new Exception( 'No study names found for result_instance_id = ${resultInstanceId}' )

		def filesDoneMap = [:]

		def selectedFilesList = ["STUDY", "CLINICAL.TXT"]//, "MRNA.TXT"]
		LinkedHashMap<String, String> snpFilesMap = new LinkedHashMap<String, String>();
		String[] parentConceptCodeList = new String[0];

		def clinicalDataFilename = getData(
				studyList,
				targetFolder,
				clinicalDefaultFilename,
				clinicalExportJobName,
				resultInstanceId,
				new String[0], //conceptCodeList,
				selectedFilesList, //["STUDY", "CLINICAL.TXT", "MRNA.TXT"], //selectedFilesList,
				true, //pivotData,
				true, //filterHighLevelConcepts,
				snpFilesMap, //snpFilesMap, // LinkedHashMap size == 0
				subset,//subset,
				filesDoneMap,
				null, // platformsList,      // GSE4382PDM
				parentConceptCodeList, //new ArrayList(), //parentConceptCodeList as String[], // ArrayList size == 0
				false //includeConceptContext
		)

		def subsetDir = targetFolder + File.separator + subset
		createDir(subsetDir)
		def convertedClinicalDataFilename = subsetDir + File.separator + convertedClinicalDataDefaultFilename
		DataConvertors.convertClinicalToOmnisoft( clinicalDataFilename, convertedClinicalDataFilename )

		// zip data
		final zipFilename = jobName + '.zip'
		final dataToZip = convertedClinicalDataFilename
		try {
			FileOutputStream fos = new FileOutputStream( tmpDir + File.separator + zipFilename )
			BufferedOutputStream bos = new BufferedOutputStream(fos)
			ZipOutputStream zippedData = new ZipOutputStream(bos)

			FileInputStream data = new FileInputStream(dataToZip)
			ZipEntry ze = new ZipEntry(convertedClinicalDataDefaultFilename)
			zippedData.putNextEntry(ze)
			zippedData << data
			zippedData.flush()
			zippedData.close()
		} catch (Exception e) {
			throw e
		}
		return jobName
	} // def getClinicalDataByResultInstanceId(

	def getData(
		studyList,
		studyDirName,
		fileName,
		jobName,
		resultInstanceId,
		conceptCodeList,
		retrievalTypes,
		parPivotData,
		parFilterHighLevelConcepts,
		snpFilesMap,
		subset,
		filesDoneMap,
		platformsList,      // GSE4382PDM
		parentConceptCodeList,
		includeConceptContext
	) {
		File studyDir = new File(studyDirName);
		def sqlQuery = new StringBuilder();
		def parameterList = null;
		String resultFileFullPath = null// = studyDir + File.separator + fileName;

		boolean retrievalTypeMRNAExists = retrievalTypeExists('MRNA', retrievalTypes)
		boolean retrievalTypeSNPExists = retrievalTypeExists('SNP', retrievalTypes)
		Boolean includeParentInfo = false

		if (null != resultInstanceId) {
			//Construct the SQL Query.
			sqlQuery <<= "SELECT ofa.PATIENT_NUM, cd.CONCEPT_PATH, cd.CONCEPT_CD, cd.NAME_CHAR, "
			sqlQuery <<= "case ofa.VALTYPE_CD "
			sqlQuery <<= " WHEN 'T' THEN TVAL_CHAR "
			sqlQuery <<= " WHEN 'N' THEN CAST(NVAL_NUM AS varchar2(30)) "
			sqlQuery <<= "END VALUE, ? SUBSET , pd.sourcesystem_cd "

			//If we are going to union in the codes that have parent concepts, we include the parent columns here too.
			if(parentConceptCodeList.size() > 0)
			{
				sqlQuery <<= ", '' AS PARENT_PATH,'' AS  PARENT_CODE "
			}

			//If we are including the concepts context, add the columns to the statement here.
			if(includeConceptContext)
			{
				sqlQuery <<= ", DC.DE_CONTEXT_NAME "
			}

			if (retrievalTypeMRNAExists && null != filesDoneMap['MRNA.TXT'] && filesDoneMap['MRNA.TXT']) {
				sqlQuery <<= ", ssm.assay_id, ssm.sample_type, ssm.timepoint, ssm.tissue_type "
			}

			sqlQuery <<= "FROM qt_patient_set_collection qt "
			sqlQuery <<= "INNER JOIN OBSERVATION_FACT ofa ON qt.PATIENT_NUM = ofa.PATIENT_NUM "

			//If we are including the concepts context, add the tables to the statement here.
			if(includeConceptContext)
			{
				sqlQuery <<= " LEFT JOIN DEAPP.DE_CONCEPT_CONTEXT DCC ON DCC.CONCEPT_CD = ofa.CONCEPT_CD "
				sqlQuery <<= " LEFT JOIN DEAPP.DE_CONTEXT DC ON DC.DE_CONTEXT_ID = DCC.DE_CONTEXT_ID "
			}

			sqlQuery <<= "INNER JOIN CONCEPT_DIMENSION cd ON cd.CONCEPT_CD = ofa.CONCEPT_CD "
			sqlQuery <<= "INNER JOIN PATIENT_DIMENSION pd on ofa.patient_num = pd.patient_num "

			if (retrievalTypeMRNAExists && null != filesDoneMap['MRNA.TXT'] && filesDoneMap['MRNA.TXT']) {
				sqlQuery <<= "LEFT JOIN DE_SUBJECT_SAMPLE_MAPPING ssm ON ssm.PATIENT_ID = ofa.PATIENT_NUM  "
			}

			sqlQuery <<= "WHERE qt.RESULT_INSTANCE_ID = ? AND ofa.MODIFIER_CD = ?"

			if (!retrievalTypeMRNAExists && parFilterHighLevelConcepts) {
				sqlQuery <<= " AND cd.concept_cd NOT IN (SELECT DISTINCT NVL(sample_type_cd,'-1') as gene_expr_concept"
				sqlQuery <<= " FROM de_subject_sample_mapping WHERE trial_name = ?"
				sqlQuery <<= " UNION SELECT DISTINCT NVL(tissue_type_cd,'-1') as gene_expr_concept "
				sqlQuery <<= " FROM de_subject_sample_mapping WHERE trial_name = ?"
				sqlQuery <<= " UNION SELECT DISTINCT NVL(platform_cd,'-1') as gene_expr_concept "
				sqlQuery <<= " FROM de_subject_sample_mapping WHERE trial_name = ?)"
			}

			if (retrievalTypeMRNAExists && null != filesDoneMap && filesDoneMap['MRNA.TXT'] && !platformsList?.isEmpty()) {
				sqlQuery <<= " AND ssm.GPL_ID IN (" << utilService.toListString(platformsList) << ") "
			}

			//If we have a list of concepts, add them to the query.
			if(conceptCodeList.size() > 0) sqlQuery <<= " AND cd.CONCEPT_CD IN (" + clinicalDataService.quoteCSV(conceptCodeList.join(",")) + ") "

			//If we have the parent codes, add the UNION to bring in the child concepts.
			if(parentConceptCodeList.size() > 0)
			{
				includeParentInfo = true

				sqlQuery <<= clinicalDataService.getParentConceptUnion(parentConceptCodeList,includeConceptContext)
			}

		}

		studyList.each { study ->
			parameterList = new ArrayList();
			//Add the name of the subset to the parameter list.
			parameterList.add(subset)
			//Add the value of the result instance ID to the parameter list.
			parameterList.add(resultInstanceId)
			//Add study to the parameter list.
			parameterList.add(study)
			if (!retrievalTypeMRNAExists && parFilterHighLevelConcepts) {
				parameterList.add(study)
				parameterList.add(study)
				parameterList.add(study)
			}

			//Add the parameters for the UNION with parent codes.
			if(parentConceptCodeList.size() > 0)
			{
				parameterList.add(subset)
				parameterList.add(resultInstanceId)
				parameterList.add(study)

				//We need to get the concept code for this path.
				String parentConceptCode = i2b2HelperService.getConceptCodeFromKey("\\\\"+parentConceptCodeList[0].trim())

				//The only use case we are concerned about for now is the case of one parent concept.
				parameterList.add(parentConceptCode)
			}

			//filename = (studyList?.size() > 1) ? study+'_'+fileName : fileName
			log.info("Retrieving Clinical data : " + sqlQuery)
			log.info("Retrieving Clinical data : " + parameterList)

			//Only pivot the data if the parameter specifies it.
			if(parPivotData)
			{
				boolean mRNAExists =  retrievalTypeMRNAExists && null != filesDoneMap['MRNA.TXT'] && filesDoneMap['MRNA.TXT']
				boolean snpExists =  retrievalTypeSNPExists && null != filesDoneMap['SNP.PED, .MAP & .CNV'] && filesDoneMap['SNP.PED, .MAP & .CNV']
				String filePath = writeData(
						sqlQuery,
						parameterList,
						studyDir,
						fileName,
						jobName,
						retrievalTypes,
						snpFilesMap
				)
				pivotData(
						(studyList?.size() > 1),
						study,
						filePath,
						mRNAExists,
						snpExists
				)
				resultFileFullPath = filePath
			}
			else
			{
				resultFileFullPath = writeData(sqlQuery, parameterList, studyDir, fileName, jobName, retrievalTypes,null,includeParentInfo,includeConceptContext)
			}
		}
		//return dataFound
		log.info('getData finished')
		log.info("getData result filename: ${fileName}")
		log.info("getData resultFileFullPath: ${resultFileFullPath}")

		return resultFileFullPath
	} // public boolean getData(c)

	private void pivotData(boolean multipleStudies, String study, String inputFileLoc, boolean mRNAExists, boolean snpExists) {
		if (StringUtils.isNotEmpty(inputFileLoc)) {
			File inputFile = new File(inputFileLoc)
			if (null != inputFile) {
				String rOutputDirectory = inputFile.getParent()
				RConnection c = new RConnection()

				//Set the working directory to be our temporary location.
				String workingDirectoryCommand = "setwd('${rOutputDirectory}')".replace("\\","\\\\")
				//Run the R command to set the working directory to our temp directory.
				REXP x = c.eval(workingDirectoryCommand)

				String rScriptDirectory = config.com.recomdata.transmart.data.export.rScriptDirectory
				String compilePivotDataCommand = ''
				if (mRNAExists) {
					compilePivotDataCommand = "source('${rScriptDirectory}/PivotData/PivotClinicalDataWithAssays2.R')"
				} else {
					compilePivotDataCommand = "source('${rScriptDirectory}/PivotData/PivotClinicalData.R')"
				}
				REXP comp = c.eval(compilePivotDataCommand)
				//Prepare command to call the PivotClinicalData.R script
				String pivotDataCommand = "PivotClinicalData.pivot('$inputFile.name', '$snpExists', '$multipleStudies', '$study')"
				//, '"+mRNAExists+"','"+snpExists+"'
				//Run the R command to pivot the data in the clinical.i2b2trans file.
				REXP pivot = c.eval(pivotDataCommand)
			}
		}
	} // private void pivotData

	private String writeData(
			StringBuilder sqlQuery,
			List parameterList,
			File studyDir,
			String fileName,
			String jobName,
			List retrievalTypes,
			Map snpFilesMap = null,
			Boolean includeParentInfo = false,
			Boolean includeConceptContext = false
	) {
		//TODO Get the dataTypeName from the list of DataTypeNames either from DB or from config file
		def dataTypeName = "Clinical";
		//TODO set this to either "Raw_Files/Findings" or NULL for processed_files
		def dataTypeFolder = null;
		//Build the query to get the clinical data.
		groovy.sql.Sql sql = new groovy.sql.Sql(dataSource)
		def char separator = '\t';
		def filePath = null
		FileWriterUtil writerUtil = null

		def dataFound

		try {
			dataFound = false


			log.info('Clinical Data Query :: ' + sqlQuery.toString())
			def rows = sql.rows(sqlQuery.toString(), parameterList)
			if (rows.size() > 0) {
				log.info('Writing Clinical File')
				writerUtil = new FileWriterUtil(studyDir, fileName, jobName, dataTypeName, dataTypeFolder, separator);
				writerUtil.writeLine(getColumnNames(retrievalTypes, snpFilesMap,includeParentInfo,includeConceptContext) as String[])

				rows.each { row ->
					dataFound = true
					def values = []
					//values.add(row.PATIENT_NUM?.toString())
					values.add(utilService.getActualPatientId(row.SOURCESYSTEM_CD?.toString()))
					values.add(row.SUBSET?.toString())
					values.add(row.CONCEPT_CD?.toString())

					//Add Concept Path
					def removalArr = [row.VALUE]
					if (retrievalTypeExists("MRNA", retrievalTypes)) {
						removalArr.add(row.TISSUE_TYPE?.toString())
						removalArr.add(row.TIMEPOINT?.toString())
						removalArr.add(row.SAMPLE_TYPE?.toString())
					}
					if (removalArr.size() == 1 && row.VALUE?.toString().equalsIgnoreCase('E')) {
						removalArr.add(row.NAME_CHAR?.toString())
					}
					values.add(ExportUtil.getShortConceptPath(row.CONCEPT_PATH, removalArr))

					if (retrievalTypeExists("MRNA", retrievalTypes)) {
						values.add(ExportUtil.getSampleValue(row.VALUE, row.SAMPLE_TYPE, row.TIMEPOINT, row.TISSUE_TYPE))
					} else {
						if (row.VALUE?.toString().equalsIgnoreCase('E')) {
							values.add(row.NAME_CHAR?.toString())
						} else {
							values.add(row.VALUE?.toString())
						}
					}

					//Actual Concept Path is required for Data Association
					values.add(row.CONCEPT_PATH)
					if (retrievalTypeExists("MRNA", retrievalTypes)) {
						values.add(row.ASSAY_ID?.toString())
					}

					if (retrievalTypeExists("SNP", retrievalTypes)) {
						def pedFile = snpFilesMap?.get("PEDFiles")?.get(row.PATIENT_NUM?.toString()+'_'+row.CONCEPT_CD?.toString())
						if (null != snpFilesMap?.get("PEDFiles")) {
							if (StringUtils.isNotEmpty(pedFile)) {
								values.add(pedFile?.toString())
							} else {
								values.add("")
							}
						}
						def mapFile = snpFilesMap?.get("MAPFiles")?.get(row.PATIENT_NUM?.toString()+'_'+row.CONCEPT_CD?.toString())
						if (null != snpFilesMap?.get("MAPFiles")) {
							if (StringUtils.isNotEmpty(mapFile)) {
								values.add(mapFile?.toString())
							} else {
								values.add("")
							}
						}
					}

					if(includeParentInfo)
					{
						values.add(row.PARENT_PATH?.toString())
						values.add(row.PARENT_CODE?.toString())
					}

					if(includeConceptContext)
					{
						values.add(row.DE_CONTEXT_NAME?.toString())
					}

					writerUtil.writeLine(values as String[])
				}
			}
			filePath = writerUtil?.outputFile?.getAbsolutePath()
		} catch (Exception e) {
			log.info(e.getMessage())
		} finally {
			writerUtil?.finishWriting()
			sql?.close()
		}
        log.info("writeData filePath=${filePath}")
		return filePath
	} // private String writeData

	def private getColumnNames(List retrievalTypes, Map snpFilesMap, Boolean includeParentInfo, Boolean includeConceptContext) {
		def columnNames = []
		columnNames.add("PATIENT ID")
		columnNames.add("SUBSET")
		columnNames.add("CONCEPT CODE")
		columnNames.add("CONCEPT PATH")
		columnNames.add("VALUE")
		columnNames.add("CONCEPT_PATH_FULL")
		if (retrievalTypeExists("MRNA", retrievalTypes)) {
			columnNames.add("ASSAY ID")
		}
		if (retrievalTypeExists("SNP", retrievalTypes)) {
			if (null != snpFilesMap?.get('PEDFiles')) columnNames.add("SNP PED File")
			if (null != snpFilesMap?.get('MAPFiles')) columnNames.add("SNP MAP File")
		}

		if (includeParentInfo) {
			columnNames.add("PARENT_PATH")
			columnNames.add("PARENT_CODE")
		}

		if (includeConceptContext) {
			columnNames.add("CONTEXT_NAME")
		}

		return columnNames
	} // def private getColumnNames

	def boolean retrievalTypeExists(String checkRetrievalType, List retrievalTypes) {
		boolean exists = false
		retrievalTypes.each { retrievalType ->
			String[] dataTypeFileType = StringUtils.split(retrievalType, ".")
			String dataType;
			if (dataTypeFileType.size() == 1) {
				dataType= retrievalType
			}
			String fileType;
			if (dataTypeFileType.size() > 1) {
				dataType = dataTypeFileType[0].trim().replace(" ","")
				fileType = dataTypeFileType[1].trim().replace(" ","")
			}
			if (dataType == checkRetrievalType) {
				exists = true
				return exists
			}
		}

		return exists
	} // def boolean retrievalTypeExists(

	/**
	 * Deletes empty spaces and quotes
	 * @param resultInstanceId
	 * @return
	 */
	private String pullOutValue(
		String resultInstanceId
	) {
		String _res = resultInstanceId.replace("\"", "");
		_res = _res.replace("'", "");
		_res = _res.trim()
		return _res
	}

	private void createDir(
	    final String dirName
	) {
		File dir = new File(dirName)
		if ( dir.exists() )
			FileUtils.deleteDirectory(dir)
		dir.mkdir();
	} // private void createDir(

	@Transactional(readOnly = true)
	def exportData(jobDataMap) {
		log.info("OmicsoftClinicalDataService.exportData started")
		Map<String, String> exportedFiles = exportDataToFiles(jobDataMap)
		final String targetFolder = jobDataMap.jobTmpDirectory

		exportedFiles.each { type, filename ->
			String subsetTargetFolder = targetFolder + File.separator + "subset" + type[type.length() - 1] + File.separator
			File subsetDir = new File(subsetTargetFolder);
			if ( !(subsetDir.exists()) )
				subsetDir.mkdir();

			if ( type.startsWith(typeNameMrna) ) {
				// Processing mrna transformation
				String convertedMrnaFilename = DataConvertors.convertMrnaToOmnisoft(filename, subsetTargetFolder)
			} else if ( type.startsWith(typeNameClinical) ) {
				// Processing clinical transformation
				String convertedClinicalFilename = DataConvertors.convertClinicalToOmnisoft(filename, subsetTargetFolder)
			}

		} // exportedFiles.each

		clearTempFolder(targetFolder)

		log.info("OmicsoftClinicalDataService.exportData finished")
	} // def exportData(jobDataMap) {


	private void clearTempFolder(
	    final String tempJobDir
	) {
		List files = new File(tempJobDir).list()
		log.info("files count : ${files.size()}")

		files.each { filename ->
			if ( filename != "subset1" && filename != "subset2" ) {
				String fullFilename = tempJobDir + File.separator + filename
				FileUtils.deleteDirectory(new File(fullFilename))
				//new File().delete(fullFilename)
			}
		}

	}

	@Transactional(readOnly = true)
	Map<String, String> exportDataToFiles(jobDataMap) {

		Map<String, String> resultFilesMap = new HashMap<String, String>(10);

		def checkboxList = jobDataMap.get('checkboxList')
		if ((checkboxList.getClass().isArray() && checkboxList?.length == 0) ||
				(checkboxList instanceof List && checkboxList?.isEmpty())) {
			throw new Exception("Please select the data to Export.");
		}
		def jobTmpDirectory = jobDataMap.get('jobTmpDirectory')
		def resultInstanceIdMap = jobDataMap.get("result_instance_ids")
		def subsetSelectedFilesMap = jobDataMap.get("subsetSelectedFilesMap")
		def subsetSelectedPlatformsByFiles = jobDataMap.get("subsetSelectedPlatformsByFiles")
		def mergeSubSet = jobDataMap.get("mergeSubset")
		//Hard-coded subsets to count 2
		def subsets = ['subset1', 'subset2']
		def study = null
		def File studyDir = null
		def filesDoneMap = [:]

		if (StringUtils.isEmpty(jobTmpDirectory)) {
			jobTmpDirectory = grailsApplication.config.com.recomdata.transmart.data.export.jobTmpDirectory
			if (StringUtils.isEmpty(jobTmpDirectory)) {
				throw new Exception('Job temp directory needs to be specified')
			}
		}

		try {
			subsets.each { subset ->
				def snpFilesMap = [:]
				def selectedFilesList = subsetSelectedFilesMap.get(subset)
				if (null != selectedFilesList && !selectedFilesList.isEmpty()) {
					//Prepare Study dir
					def List studyList = null
					if (null != resultInstanceIdMap[subset] && !resultInstanceIdMap[subset].isEmpty()) {
						studyList = i2b2ExportHelperService.findStudyAccessions([resultInstanceIdMap[subset]])
						if (!studyList.isEmpty()) {
							study = studyList.get(0)
							studyDir = new File(jobTmpDirectory, subset + (studyList.size() == 1 ? '_'+study : ''))
							studyDir.mkdir()
						}
					}

					//Pull the data pivot parameter out of the data map.
					def pivotDataValueDef = jobDataMap.get("pivotData")
					boolean pivotData = new Boolean(true)

					if(pivotDataValueDef==false) pivotData = new Boolean(false)
					boolean writeClinicalData = false
					if(null != resultInstanceIdMap[subset] && !resultInstanceIdMap[subset].isEmpty())
					{
						// Construct a list of the URL objects we're running, submitted to the pool
						selectedFilesList.each() { selectedFile ->

							if (StringUtils.equalsIgnoreCase(selectedFile, "CLINICAL.TXT")) {
								writeClinicalData = true
							}

							log.info("selectedFile = $selectedFile")

							log.info('Working on to export File :: ' + selectedFile)
							def List gplIds = subsetSelectedPlatformsByFiles?.get(subset)?.get(selectedFile)
							def retVal = null
							switch (selectedFile)
							{
								case "STUDY":
									retVal = metadataService.getData(studyDir, "experimentalDesign.txt", jobDataMap.get("jobName"), studyList);
									log.info("retrieved study data")
									break;
								case "MRNA.TXT":
									retVal = geneExpressionDataService.getData(studyList, studyDir, "mRNA.trans", jobDataMap.get("jobName"), resultInstanceIdMap[subset], pivotData, gplIds, null, null, null, null, false);

									String mrnaFilename = studyDir.absolutePath + '/mRNA/Processed_Data/mRNA_trans.txt'
									String key = subset.equalsIgnoreCase("subset1") ? typeNameMrna + "1" : typeNameMrna + "2";
									resultFilesMap.put(key, mrnaFilename)

									//filesDoneMap is used for building the Clinical Data query
									filesDoneMap.put('MRNA.TXT', new Boolean(true))
									break;
								case "MRNA_DETAILED.TXT":
									//We need to grab some inputs from the jobs data map.
									def pathway 	= jobDataMap.get("gexpathway")
									def timepoint 	= jobDataMap.get("gextime")
									def sampleType 	= jobDataMap.get("gexsample")
									def tissueType	= jobDataMap.get("gextissue")
									def gplString   = jobDataMap.get("gexgpl")

									if(tissueType == ",") tissueType = ""
									if(sampleType == ",") sampleType = ""
									if(timepoint == ",") timepoint = ""

									if(gplIds != null)
									{
										gplIds 			= gplString.tokenize(",")
									}
									else
									{
										gplIds = []
									}

									//adding String to a List to make it compatible to the type expected
									//if gexgpl contains multiple gpl(s) as single string we need to convert that to a list

									retVal = geneExpressionDataService.getData(studyList, studyDir, "mRNA.trans", jobDataMap.get("jobName"), resultInstanceIdMap[subset], pivotData, gplIds , pathway, timepoint, sampleType,tissueType,true)
									if(jobDataMap.get("analysis") != "DataExport") {
										//if geneExpressionDataService was not able to find data throw an exception.
										if(!retVal) {
											throw new DataNotFoundException("There are no patients that meet the criteria selected therefore no gene expression data was returned.")
										}
									}
									break;
								case "MRNA.CEL":
									geneExpressionDataService.downloadCELFiles(resultInstanceIdMap[subset], studyList, studyDir, jobDataMap.get("jobName"), null, null, null, null)
									break;
								case "GSEA.GCT & .CLS":
									geneExpressionDataService.getGCTAndCLSData(studyList, studyDir, "mRNA.GCT", jobDataMap.get("jobName"), resultInstanceIdMap, pivotData, gplIds)
									break;
								case "SNP.PED, .MAP & .CNV":
									retVal = snpDataService.getData(studyDir, "snp.trans", jobDataMap.get("jobName"), resultInstanceIdMap[subset])
									snpDataService.getDataByPatientByProbes(studyDir, resultInstanceIdMap[subset], jobDataMap.get("jobName"))
									break;
								case "SNP.CEL":
									snpDataService.downloadCELFiles(studyList, studyDir, resultInstanceIdMap[subset], jobDataMap.get("jobName"))
									break;
								case "SNP.TXT":
									//In this case we need to get a file with Patient ID, Probe ID, Gene, Genotype, Copy Number
									//We need to grab some inputs from the jobs data map.
									def pathway = jobDataMap.get("snppathway")
									def sampleType = jobDataMap.get("snptime")
									def timepoint = jobDataMap.get("snpsample")
									def tissueType = jobDataMap.get("snptissue")

									//This object will be our row processor which handles the writing to the SNP text file.
									SnpData snpData = new SnpData()
									//Construct the path that we create the SNP file on.
									def SNPFolderLocation = jobTmpDirectory + File.separator + "subset1_${study}" + File.separator + "SNP" + File.separator
									//Make sure the directory we want to write the file to is created.
									def snpDir = new File(SNPFolderLocation)
									snpDir.mkdir()
									//This is the exact path of the file to write.
									def fileLocation = jobTmpDirectory + File.separator + "subset1_${study}" + File.separator + "SNP" + File.separator + "snp.trans"
									//Call our service which writes the SNP data to a file.
									Boolean gotData = snpDataService.getSnpDataByResultInstanceAndGene(resultInstanceIdMap[subset],study,pathway,sampleType,timepoint,tissueType,snpData, fileLocation,true,true)
									if(jobDataMap.get("analysis") != "DataExport") {
										//if SNPDataService was not able to find data throw an exception.
										if(!gotData) {
											throw new DataNotFoundException("There are no patients that meet the criteria selected therefore no SNP data was returned.")
										}
									}
									break;
								case "ADDITIONAL":
									additionalDataService.downloadFiles(resultInstanceIdMap[subset], studyList, studyDir, jobDataMap.get("jobName"))
									break;
								case "IGV.VCF":

									def selectedGenes = jobDataMap.get("selectedGenes")
									def chromosomes = jobDataMap.get("chroms")
									def selectedSNPs = jobDataMap.get("selectedSNPs")

									log.info("VCF Parameters")
									log.info("selectedGenes:" + selectedGenes)
									log.info("chromosomes:" + chromosomes)
									log.info("selectedSNPs:" + selectedSNPs)



									//def IGVFolderLocation = jobTmpDirectory + File.separator + "subset1_${study}" + File.separator + "VCF" + File.separator

									//
									//	def outputDir = "/users/jliu/tmp"
									def outputDir = grailsApplication.config.com.recomdata.analysis.data.file.dir;
									def webRootName = jobDataMap.get("appRealPath");
									if (webRootName.endsWith(File.separator) == false)
										webRootName += File.separator;
									outputDir =  webRootName + outputDir;
									def prefix = "S1"
									if('subset2'==subset)
										prefix = "S2"
									vcfDataService.getDataAsFile(outputDir, jobDataMap.get("jobName"), null, resultInstanceIdMap[subset], selectedSNPs, selectedGenes, chromosomes, prefix);
									break;
							}

							log.info("retVal: "+retVal)
							log.info("====================")

						}
					}

					if (writeClinicalData) {

						//Grab the item from the data map that tells us whether we need the concept contexts.
						Boolean includeConceptContext = jobDataMap.get("includeContexts",false);

						//This is a list of concept codes that we use to filter the result instance id results.
						String[] conceptCodeList = jobDataMap.get("concept_cds");

						//This is list of concept codes that are parents to some child concepts. We need to expand these out in the service call.
						List parentConceptCodeList = new ArrayList()

						if(jobDataMap.get("parentNodeList",null) != null)
						{
							//This variable tells us which variable actually holds the parent concept code.
							String conceptVariable = jobDataMap.get("parentNodeList")

							//Get the actual concept value from the map.
							parentConceptCodeList.add(jobDataMap.get(conceptVariable))
						}
						else
						{
							parentConceptCodeList = []
						}

						//Make this blank instead of null if we don't find it.
						if(conceptCodeList == null)	conceptCodeList = []

						//Set the flag that tells us whether or not to exclude the high level concepts. Should this logic even be in the DAO?
						boolean filterHighLevelConcepts = false


						String clinicalFilename = studyDir.absolutePath + '/Clinical/clinical_i2b2trans.txt'
						String key = subset.equalsIgnoreCase("subset1") ? typeNameClinical + "1" : typeNameClinical + "2";
						resultFilesMap.put(key, clinicalFilename)


						if(jobDataMap.get("analysis") == "DataExport") filterHighLevelConcepts = true
						def platformsList = subsetSelectedPlatformsByFiles?.get(subset)?.get("MRNA.TXT")
						//Reason for moving here: We'll get the map of SNP files from SnpDao to be output into Clinical file
						def retVal = clinicalDataService.getData(studyList, studyDir, "clinical.i2b2trans", jobDataMap.get("jobName"),
								resultInstanceIdMap[subset], conceptCodeList, selectedFilesList, pivotData, filterHighLevelConcepts,
								snpFilesMap, subset, filesDoneMap, platformsList, parentConceptCodeList as String[], includeConceptContext)

						if(jobDataMap.get("analysis") != "DataExport") {
							//if i2b2Dao was not able to find data for any of the studies associated with the result instance ids, throw an exception.
							if(!retVal)	{
								throw new DataNotFoundException("There are no patients that meet the criteria selected therefore no clinical data was returned.")
							}
						}
					} // if (writeClinicalData) {
				} // if (null != selectedFilesList && !selectedFilesList.isEmpty()) {
			} // subsets.each { subset ->
		} catch (Exception e) {
			throw new Exception(e.message ? e.message : (e.cause?.message ? e.cause?.message : ''), e)
		} // try

		log.info("exportData finished")
		return resultFilesMap;
	} // def exportData(jobDataMap) {

	private String specifySubset(
		String providedSubsetName
	) throws Exception {
		if ( providedSubsetName.indexOf("1") != -1 && providedSubsetName.indexOf("2") == -1 ) {
			return subset1;
		} else if ( providedSubsetName.indexOf("1") == -1 && providedSubsetName.indexOf("2") != -1 ) {
			return subset2;
		} else {
			throw new Exception('Cannot define subset provided: can be only \'${subset1}\' or \'${subset2}\'. Provided subset name: \'${providedSubsetName}\'')
		}


		return null;
	}
}
