package com.recomdata.transmart.data.export.omicsoftIntegration

import com.recomdata.dataexport.util.ExportUtil
import com.recomdata.snp.SnpData
import com.recomdata.transmart.data.export.DataExportService
import com.recomdata.transmart.data.export.exception.DataNotFoundException
import com.recomdata.transmart.data.export.util.FileWriterUtil
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.codehaus.groovy.grails.commons.ConfigurationHolder
import org.rosuda.REngine.REXP
import org.rosuda.REngine.Rserve.RConnection
import org.springframework.transaction.annotation.Transactional

import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

/**
 * Created by transmart on 4/21/14.
 */
class OmicsoftProjectDataService extends DataExportService {
    final String typeNameMrna = 'mrna'
    final String typeNameClinical = 'clinical'
    final String subset1 = 'subset1'
    final String subset2 = 'subset2'
    final String clinicalDefaultFilename = 'clinical_i2b2trans' // DO NOT CHANGE!!! HARDCOD UNDER.
    final String clinicalExportJobName = 'omicsoftProjectExport'
    final String convertedClinicalDataDefaultFilename = 'clinical.design.txt'
    final String projectConvertorDefaultPath = '/ProjectConverter'
    // consts (end)

    // services (start)
    def clinicalDataService
    def i2b2ExportHelperService
    def grailsApplication
    def utilService
    def i2b2HelperService
    def config = ConfigurationHolder.config
    def sqlUtilsService

    private static final String PROBE_ID = "PROBE ID"
    def dataSource;
    def dataCountService

    final String convertedExpressionDataDefaultFilename = 'biomarker.expression.txt';
    //final String tempDir = config.com.recomdata.plugins.tempFolderDirectory;
    final String convertedPlatformDataDefaultFilename = 'annotation.txt';
    final String metaDataDefaultFilename = 'metadata.txt';
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
    String getProjectDataByResultInstanceId(String _resultInstanceId, String _subset) throws Exception {
        String resultInstanceId = pullOutValue(_resultInstanceId)
        String subset = specifySubset(pullOutValue(_subset))
        // managing temp directories
        //def tmpDir = grailsApplication.config.com.recomdata.transmart.data.export.jobTmpDirectory
        def tmpDir = grailsApplication.config.com.recomdata.plugins.tempFolderDirectory
        //def tmpDir = grailsApplication.config.com.recomdata.plugins.tempFolderDirectory
        def jobName = sqlUtilsService.generateOmicsoftJobName()
        //'omicsoftExport_' + (new Date().format('yyyy_MM_dd__HH_mm_ss'))
        def targetFolder = tmpDir + File.separator + jobName + File.separator
        createDir(targetFolder)

        // get study names for provided result_instance_id
        def studyList = i2b2ExportHelperService.findStudyAccessions([resultInstanceId])

        if (studyList.size() == 0) {
            log.info("No study names found for result_instance_id = ${resultInstanceId}")
            return 'No study names found'
        }

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
        if (clinicalDataFilename.equals('Empty clinical data')) {
            log.info("Empty clinical data")
            return 'Empty clinical data'
        }
        log.info("clinicalDataFilename=${clinicalDataFilename}")

        def subsetDir = targetFolder + File.separator + subset
        log.info("subsetDir=" + subsetDir)
        createDir(subsetDir)
        def convertedClinicalDataFilename = subsetDir + File.separator + convertedClinicalDataDefaultFilename
        DataConvertors.convertClinicalToOmnisoft(clinicalDataFilename, convertedClinicalDataFilename)
        log.info("convertClinicalToOmnisoft finished")

        def con, stmt, stmt1, annotationStmt, rs;

        // Grab the connection from the grails object.
        con = dataSource.getConnection();

        // Grab the configuration that sets the fetch size.
        def rsize = config.com.recomdata.plugins.resultSize;
        Integer fetchSize = 5000;
        if (rsize != null) {
            try {
                fetchSize = Integer.parseInt(rsize);
            } catch (Exception exs) {
                log.warn("com.recomdata.plugins.resultSize is not set!");

            }
        }
        // resultInstanceId = '19728';

        // Get data for export file header
        def sqlQuery = """SELECT DISTINCT
                ssm.assay_id,
                ssm.sample_type,
                ssm.sample_cd,
                ssm.GPL_ID,
                ssm.platform
                FROM de_subject_sample_mapping ssm
                INNER JOIN qt_patient_set_collection sc ON sc.result_instance_id = ? AND ssm.patient_id = sc.patient_num ORDER BY assay_id"""
        //WHERE ssm.trial_name = 'GSE4382'  AND ssm.GPL_ID IN ('GSE4382PDM') ORDER BY assay_id"""

        // Get samples data
        def sampleQuery = """SELECT
                a.LOG_INTENSITY,
                a.assay_id,
                b.probe_id,
                b.probeset_id,
                b.GENE_SYMBOL,
                b.GENE_ID,
                pd.sourcesystem_cd,
                ssm.gpl_id
                FROM de_subject_microarray_data a
                INNER JOIN de_subject_sample_mapping ssm ON ssm.assay_id = A.assay_id
                INNER JOIN de_mrna_annotation b ON a.probeset_id = b.probeset_id and ssm.gpl_id = b.gpl_id
                INNER JOIN qt_patient_set_collection sc ON sc.result_instance_id = ? AND ssm.PATIENT_ID = sc.patient_num
                INNER JOIN PATIENT_DIMENSION pd on ssm.patient_id = pd.patient_num ORDER BY probe_id, assay_id, gpl_id"""
        //WHERE SSM.trial_name = 'GSE4382'  AND ssm.GPL_ID IN ('GSE4382PDM') ORDER BY probe_id, assay_id, gpl_id"""

        //Get annotation data
        def annotationQuery = """SELECT DISTINCT
                a.PROBE_ID,a.GENE_SYMBOL
                from deapp.de_mrna_annotation a
                INNER JOIN de_subject_sample_mapping ssm ON ssm.GPL_ID = a.gpl_id
                INNER JOIN qt_patient_set_collection sc ON sc.result_instance_id = ? AND ssm.patient_id = sc.patient_num"""
        // Prepare the SQL statement.
        stmt = con.prepareStatement(sqlQuery);
        stmt.setString(1, resultInstanceId);
        stmt.setFetchSize(fetchSize)

        // Sample query
        stmt1 = con.prepareStatement(sampleQuery);
        stmt1.setString(1, resultInstanceId);
        stmt1.setFetchSize(fetchSize);

        annotationStmt = con.prepareStatement(annotationQuery);
        annotationStmt.setString(1, resultInstanceId);
        annotationStmt.setFetchSize(fetchSize);

        def sampleType, sampleCD, assayID, GPL_ID, logIntensity, probeId, prevProbeId = null, samplePlatform, geneSymbol = null;
        def char separator = '\t';

        // Load header data
        log.info("Start header data retrieving query");
        log.info("Header query : " + sqlQuery);
        rs = stmt.executeQuery();
        //def headerMap = [:];
        StringBuilder headerString = new StringBuilder();
        headerString.append(PROBE_ID).append(separator);
        StringBuilder headerStringPlatform = new StringBuilder();
        headerStringPlatform.append(PROBE_ID).append(separator);
        StringBuilder tmpString = new StringBuilder();

        boolean emptyQueryResult

        try {
            emptyQueryResult = true;
            while (rs.next()) {
                tmpString = new StringBuilder();
                sampleType = rs.getString("SAMPLE_TYPE");
                sampleCD = rs.getString("SAMPLE_CD");
                assayID = rs.getString("ASSAY_ID");
                GPL_ID = rs.getString("GPL_ID");
                // samplePlatform = rs.getString("PLATFORM");

                tmpString = StringUtils.isNotEmpty(sampleCD) && StringUtils.isNotEmpty(sampleType) && StringUtils.isNotEmpty(GPL_ID) ?
                        tmpString.append(sampleCD).append("_").append(sampleType).append("_").append(GPL_ID) : null;

                if (assayID != null && tmpString.length() > 0) {
                    //headerMap.put(assayID, tmpString);
                    headerString.append(tmpString).append(separator);
                }
                emptyQueryResult = false
            }
        } finally {
            rs?.close();
            stmt?.close();
            if (emptyQueryResult) {
                return 'Empty data for export file header'
            }
        }
        log.info("Finished header data retrieving query");

        if (headerString.toString().trim().equals(PROBE_ID)) {
            log.info("Error:headerString.toString().trim().equals(PROBE_ID)");
            return "Empty headerString";
        }
        // Create export file
        //String fileSeparator = File.separator;
        // String folderName = "omicsoftProjectExport" + new Date().format('yyyy_MM_dd__HH_mm_ss') + fileSeparator + subset + fileSeparator;
        // Check folder
        // File subsetFolder = new File(tempDir + folderName);
        // createDir(subsetFolder);

        File exportFile = new File(subsetDir + File.separator + convertedExpressionDataDefaultFilename);
        BufferedWriter bw = new BufferedWriter(new FileWriter(exportFile.getAbsolutePath()));
        def convertedHeaderString = DataConvertors.convertExpressionHeader(headerString.toString())
        bw.writeLine(convertedHeaderString);

        File platformFile = new File(subsetDir + File.separator + convertedPlatformDataDefaultFilename)
        BufferedWriter bwPlatform = new BufferedWriter(new FileWriter(platformFile.getAbsolutePath()));
        bwPlatform.writeLine("PROBE_ID" + separator + "GENE_SYMBOL");

        File metadataFile = new File(subsetDir + File.separator + metaDataDefaultFilename)
        BufferedWriter bwMetadata = new BufferedWriter(new FileWriter(metadataFile.getAbsolutePath()));
        bwMetadata.writeLine("unknown");

        // Load sample data
        log.info("Start sample retrieving query");
        log.info("Sample Query : " + sampleQuery);
        rs = stmt1.executeQuery();
        tmpString = new StringBuilder();

        try {
            emptyQueryResult = true
            while (rs.next()) {
                probeId = rs.getString("PROBE_ID");
                assayID = rs.getString("ASSAY_ID");
                logIntensity = rs.getDouble("LOG_INTENSITY");

                if (prevProbeId != null && !prevProbeId.equals(probeId)) {
                    bw.writeLine(prevProbeId + separator + tmpString.toString());
                    tmpString = new StringBuilder();
                }
                tmpString.append(logIntensity).append(separator);
                prevProbeId = probeId;
                emptyQueryResult = false
            }
        } finally {
            log.info("Write in file line: " + prevProbeId + separator + tmpString.toString())
            bw.writeLine(prevProbeId + separator + tmpString.toString());
            bw.flush();
            bw.close();
            bwMetadata.flush();
            bwMetadata.close();
            stmt1?.close();
            rs?.close();
            if (emptyQueryResult) {
                return 'Empty samples data'
            }
        }

        rs = annotationStmt.executeQuery();
        try {
            emptyQueryResult = true
            while (rs.next()) {
                probeId = rs.getString("PROBE_ID");
                geneSymbol = rs.getString("GENE_SYMBOL");
                //log.info("Write at platform: "+ probeId + separator + geneSymbol)
                bwPlatform.writeLine(probeId + separator + geneSymbol);
                emptyQueryResult = false
            }
        } finally {
            bwPlatform.flush();
            bwPlatform.close();
            rs?.close();
            annotationStmt?.close();
            if (emptyQueryResult) {
                return 'Empty annotation file'
            }
        }

        log.info("Finished sample retrieving query");
        log.info("Finished header data retrieving query");

        def projectConverterPath = grailsApplication.config.com.recomdata.plugins.projectCoverterPath
        log.info("projectConvertorPath = ${projectConverterPath}");

        try {
            String sourceFilePath = projectConverterPath ?:
                    org.codehaus.groovy.grails.web.context.ServletContextHolder.getServletContext().getRealPath(projectConvertorDefaultPath);
            String destinationFilePath = subsetDir.toString()
            log.info("copy from ${sourceFilePath} to ${destinationFilePath}");
            File srcDir = new File(sourceFilePath)
            File destDir = new File(destinationFilePath)
            log.info("srcDir exists: ${srcDir.exists()}, destDir exists: ${destDir.exists()}")
            FileUtils.copyDirectory(srcDir, destDir)
            /*new AntBuilder().copy(todir: destinationFilePath) {
                fileset(dir: sourceFilePath)
            }*/
            log.info("Finished copy");
        } catch (Exception e) {
            log.error(e.getMessage(), e)
            return "Error copying ProjectConverter.exe"
        }

        log.info("Starting converting");
        /*try {
            def fiii = download("http://www.omicsoft.com/downloads/annotation/__Old/GEO.GPL96.annotation3","/home/transmart/Desktop/vvv.txt")
        } catch(Exception e) {
            log.error(e.getMessage(), e)
        }*/
        String projectFileName = getProjectFileName(studyList.get(0), resultInstanceId)
        log.info("projectFileName=${projectFileName}")

            String omicsoftExportCmd = "mono " +
                    subsetDir + File.separator + "ProjectConverter.exe -m " +
                    subsetDir + File.separator + convertedExpressionDataDefaultFilename + " " +
                    subsetDir + File.separator + convertedClinicalDataDefaultFilename + " " +
                    subsetDir + File.separator + convertedPlatformDataDefaultFilename + " " +
                    subsetDir + File.separator + metaDataDefaultFilename + " " +
                    targetFolder + File.separator + projectFileName;
            log.info("CMD:${omicsoftExportCmd}")
//            def creatingProjectFileCommand = Runtime.getRuntime().exec(omicsoftExportCmd);
//            creatingProjectFileCommand.waitFor()                               // Wait for the command to finish
            String line;
        try {
            Process p = Runtime.getRuntime().exec( omicsoftExportCmd);

            BufferedReader inStream = new BufferedReader(
                    new InputStreamReader(p.getInputStream()) );
            while ((line = inStream.readLine()) != null) {
                log.info(line);
                if (line.contains("Error occured")){
                    log.error('Exception in data processing in ProjectConverter')
                    return 'Exception in data processing in ProjectConverter'
                }
            }
            log.info("mono complete");
            inStream.close();
        }
        catch (Exception e) {
            log.error(e.getMessage(), e)
            return 'Can not find mono'
        }
        // zip data
        String zipFilename = jobName + '.zip'
        ZipOutputStream zippedData = null
        try {
            File projectFile = new File(targetFolder + File.separator + projectFileName);
            zippedData = new ZipOutputStream(new FileOutputStream(tmpDir + File.separator + zipFilename))
            FileInputStream fileInputStream = new FileInputStream(projectFile)
            ZipEntry ze = new ZipEntry(projectFileName)
            zippedData.putNextEntry(ze)
            zippedData << fileInputStream
            zippedData.flush()
        } finally {
            if (zippedData != null) {
                zippedData.close()
            }
        }
        return jobName
    }// def getProjectDataByResultInstanceId


    def getProjectFileName(studyName, resultInstaceId) {
        def subsetCountMap = dataCountService.getDataCounts(resultInstaceId)
        def projectFileName = studyName + '_' + subsetCountMap['CLINICAL'] + '_' + new Date().format('yyyy_MM_dd__HH_mm_ss') + '.osprj'
        projectFileName
    }

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
            if (parentConceptCodeList.size() > 0) {
                sqlQuery <<= ", '' AS PARENT_PATH,'' AS  PARENT_CODE "
            }

            //If we are including the concepts context, add the columns to the statement here.
            if (includeConceptContext) {
                sqlQuery <<= ", DC.DE_CONTEXT_NAME "
            }

            // if (retrievalTypeMRNAExists && null != filesDoneMap['MRNA.TXT'] && filesDoneMap['MRNA.TXT']) {
            sqlQuery <<= ", ssm.assay_id, ssm.sample_type, ssm.timepoint, ssm.tissue_type, ssm.sample_cd  "
            //}

            sqlQuery <<= "FROM qt_patient_set_collection qt "
            sqlQuery <<= "INNER JOIN OBSERVATION_FACT ofa ON qt.PATIENT_NUM = ofa.PATIENT_NUM "

            //If we are including the concepts context, add the tables to the statement here.
            if (includeConceptContext) {
                sqlQuery <<= " LEFT JOIN DEAPP.DE_CONCEPT_CONTEXT DCC ON DCC.CONCEPT_CD = ofa.CONCEPT_CD "
                sqlQuery <<= " LEFT JOIN DEAPP.DE_CONTEXT DC ON DC.DE_CONTEXT_ID = DCC.DE_CONTEXT_ID "
            }

            sqlQuery <<= "INNER JOIN CONCEPT_DIMENSION cd ON cd.CONCEPT_CD = ofa.CONCEPT_CD "
            sqlQuery <<= "INNER JOIN PATIENT_DIMENSION pd on ofa.patient_num = pd.patient_num "

            // if (retrievalTypeMRNAExists && null != filesDoneMap['MRNA.TXT'] && filesDoneMap['MRNA.TXT']) {
            sqlQuery <<= "LEFT JOIN DE_SUBJECT_SAMPLE_MAPPING ssm ON ssm.PATIENT_ID = ofa.PATIENT_NUM  "
            //}

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
            if (conceptCodeList.size() > 0) sqlQuery <<= " AND cd.CONCEPT_CD IN (" + clinicalDataService.quoteCSV(conceptCodeList.join(",")) + ") "

            //If we have the parent codes, add the UNION to bring in the child concepts.
            if (parentConceptCodeList.size() > 0) {
                includeParentInfo = true

                sqlQuery <<= clinicalDataService.getParentConceptUnion(parentConceptCodeList, includeConceptContext)
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
            if (parentConceptCodeList.size() > 0) {
                parameterList.add(subset)
                parameterList.add(resultInstanceId)
                parameterList.add(study)

                //We need to get the concept code for this path.
                String parentConceptCode = i2b2HelperService.getConceptCodeFromKey("\\\\" + parentConceptCodeList[0].trim())

                //The only use case we are concerned about for now is the case of one parent concept.
                parameterList.add(parentConceptCode)
            }

            //filename = (studyList?.size() > 1) ? study+'_'+fileName : fileName
            log.info("Retrieving Clinical data : " + sqlQuery)
            log.info("Retrieving Clinical data : " + parameterList)

            //Only pivot the data if the parameter specifies it.
            if (parPivotData) {
                boolean mRNAExists = retrievalTypeMRNAExists && null != filesDoneMap['MRNA.TXT'] && filesDoneMap['MRNA.TXT']
                boolean snpExists = retrievalTypeSNPExists && null != filesDoneMap['SNP.PED, .MAP & .CNV'] && filesDoneMap['SNP.PED, .MAP & .CNV']
                String filePath = writeData(
                        sqlQuery,
                        parameterList,
                        studyDir,
                        fileName,
                        jobName,
                        retrievalTypes,
                        snpFilesMap
                )
                if (filePath.equals('Empty clinical data')) {
                    log.info("Empty clinical data")
                    return 'Empty clinical data'
                }
                pivotData(
                        (studyList?.size() > 1),
                        study,
                        filePath,
                        mRNAExists,
                        snpExists
                )
                log.info("pivotData finished")
                resultFileFullPath = filePath
            } else {
                resultFileFullPath = writeData(sqlQuery, parameterList, studyDir, fileName, jobName, retrievalTypes, null, includeParentInfo, includeConceptContext)
                if (resultFileFullPath.equals('Empty clinical data')) {
                    log.info("Empty clinical data")
                    return 'Empty clinical data'
                }
            }
        }
        //return dataFound
        log.info('getData finished')
        log.info("result filename: ${fileName}")
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
                String workingDirectoryCommand = "setwd('${rOutputDirectory}')".replace("\\", "\\\\")
                //Run the R command to set the working directory to our temp directory.
                REXP x = c.eval(workingDirectoryCommand)

                String rScriptDirectory = config.com.recomdata.transmart.data.export.rScriptDirectory
                log.info("pivotData rScriptDirectory=${rScriptDirectory}")
                String compilePivotDataCommand = ''
                if (mRNAExists) {
                    compilePivotDataCommand = "source('${rScriptDirectory}/PivotData/PivotClinicalDataWithAssays2.R')"
                } else {
                    compilePivotDataCommand = "source('${rScriptDirectory}/PivotData/PivotClinicalData.R')"
                }
                log.info("pivotData compilePivotDataCommand=${compilePivotDataCommand}")
                REXP comp = c.eval(compilePivotDataCommand)
                //Prepare command to call the PivotClinicalData.R script
                String pivotDataCommand = "PivotClinicalData.pivot('$inputFile.name', '$snpExists', '$multipleStudies', '$study')"
                //, '"+mRNAExists+"','"+snpExists+"'
                //Run the R command to pivot the data in the clinical.i2b2trans file.
                log.info("pivotData pivotDataCommand=${pivotDataCommand}")
                REXP pivot = c.eval(pivotDataCommand)
                int ii = 0
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
            log.info("rows.size()= " + rows.size())
            if (rows.size() > 0) {
                log.info('Writing Clinical File')
                writerUtil = new FileWriterUtil(studyDir, fileName, jobName, dataTypeName, dataTypeFolder, separator);
                writerUtil.writeLine(getColumnNames(retrievalTypes, snpFilesMap, includeParentInfo, includeConceptContext) as String[])
                // String headTypes=""
                // (getColumnNames(retrievalTypes, snpFilesMap,includeParentInfo,includeConceptContext)as String[]).each {
                //    headTypes+="\t"
                // }
                // headTypes=headTypes-"\t"
                // writerUtil.writeLine(headTypes.toString())

                rows.each { row ->
                    dataFound = true
                    def values = []
                    //values.add(row.PATIENT_NUM?.toString())
                    // values.add(utilService.getActualPatientId(row.SOURCESYSTEM_CD?.toString()))
                    //log.info("row="+row)
                    values.add(row.SAMPLE_CD?.toString())
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
                        def pedFile = snpFilesMap?.get("PEDFiles")?.get(row.PATIENT_NUM?.toString() + '_' + row.CONCEPT_CD?.toString())
                        if (null != snpFilesMap?.get("PEDFiles")) {
                            if (StringUtils.isNotEmpty(pedFile)) {
                                values.add(pedFile?.toString())
                            } else {
                                values.add("")
                            }
                        }
                        def mapFile = snpFilesMap?.get("MAPFiles")?.get(row.PATIENT_NUM?.toString() + '_' + row.CONCEPT_CD?.toString())
                        if (null != snpFilesMap?.get("MAPFiles")) {
                            if (StringUtils.isNotEmpty(mapFile)) {
                                values.add(mapFile?.toString())
                            } else {
                                values.add("")
                            }
                        }
                    }

                    if (includeParentInfo) {
                        values.add(row.PARENT_PATH?.toString())
                        values.add(row.PARENT_CODE?.toString())
                    }

                    if (includeConceptContext) {
                        values.add(row.DE_CONTEXT_NAME?.toString())
                    }

                    writerUtil.writeLine(values as String[])
                }
                log.info('Writing Clinical File end')
            } else {
                log.info("Writing Clinical writeData() :: empty clinical data")
                return 'Empty clinical data'
            }

            filePath = writerUtil?.outputFile?.getAbsolutePath()
            log.info("Writing Clinical filePath=${filePath}")

        } catch (Exception e) {
            log.info(e.getMessage())
            throw e
        } finally {
            writerUtil?.finishWriting()
            sql?.close()
        }
        log.info("filePath= " + filePath)
        return filePath
    } // private String writeData

    def
    private getColumnNames(List retrievalTypes, Map snpFilesMap, Boolean includeParentInfo, Boolean includeConceptContext) {
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
                dataType = retrievalType
            }
            String fileType;
            if (dataTypeFileType.size() > 1) {
                dataType = dataTypeFileType[0].trim().replace(" ", "")
                fileType = dataTypeFileType[1].trim().replace(" ", "")
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
        if (dir.exists())
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
            if (!(subsetDir.exists()))
                subsetDir.mkdir();

            if (type.startsWith(typeNameMrna)) {
                // Processing mrna transformation
                String convertedMrnaFilename = DataConvertors.convertMrnaToOmnisoft(filename, subsetTargetFolder)
            } else if (type.startsWith(typeNameClinical)) {
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
            if (filename != "subset1" && filename != "subset2") {
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
                            studyDir = new File(jobTmpDirectory, subset + (studyList.size() == 1 ? '_' + study : ''))
                            studyDir.mkdir()
                        }
                    }

                    //Pull the data pivot parameter out of the data map.
                    def pivotDataValueDef = jobDataMap.get("pivotData")
                    boolean pivotData = new Boolean(true)

                    if (pivotDataValueDef == false) pivotData = new Boolean(false)
                    boolean writeClinicalData = false
                    if (null != resultInstanceIdMap[subset] && !resultInstanceIdMap[subset].isEmpty()) {
                        // Construct a list of the URL objects we're running, submitted to the pool
                        selectedFilesList.each() { selectedFile ->

                            if (StringUtils.equalsIgnoreCase(selectedFile, "CLINICAL.TXT")) {
                                writeClinicalData = true
                            }

                            log.info("selectedFile = $selectedFile")

                            log.info('Working on to export File :: ' + selectedFile)
                            def List gplIds = subsetSelectedPlatformsByFiles?.get(subset)?.get(selectedFile)
                            def retVal = null
                            switch (selectedFile) {
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
                                    def pathway = jobDataMap.get("gexpathway")
                                    def timepoint = jobDataMap.get("gextime")
                                    def sampleType = jobDataMap.get("gexsample")
                                    def tissueType = jobDataMap.get("gextissue")
                                    def gplString = jobDataMap.get("gexgpl")

                                    if (tissueType == ",") tissueType = ""
                                    if (sampleType == ",") sampleType = ""
                                    if (timepoint == ",") timepoint = ""

                                    if (gplIds != null) {
                                        gplIds = gplString.tokenize(",")
                                    } else {
                                        gplIds = []
                                    }

                                    //adding String to a List to make it compatible to the type expected
                                    //if gexgpl contains multiple gpl(s) as single string we need to convert that to a list

                                    retVal = geneExpressionDataService.getData(studyList, studyDir, "mRNA.trans", jobDataMap.get("jobName"), resultInstanceIdMap[subset], pivotData, gplIds, pathway, timepoint, sampleType, tissueType, true)
                                    if (jobDataMap.get("analysis") != "DataExport") {
                                        //if geneExpressionDataService was not able to find data throw an exception.
                                        if (!retVal) {
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
                                    Boolean gotData = snpDataService.getSnpDataByResultInstanceAndGene(resultInstanceIdMap[subset], study, pathway, sampleType, timepoint, tissueType, snpData, fileLocation, true, true)
                                    if (jobDataMap.get("analysis") != "DataExport") {
                                        //if SNPDataService was not able to find data throw an exception.
                                        if (!gotData) {
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
                                    outputDir = webRootName + outputDir;
                                    def prefix = "S1"
                                    if ('subset2' == subset)
                                        prefix = "S2"
                                    vcfDataService.getDataAsFile(outputDir, jobDataMap.get("jobName"), null, resultInstanceIdMap[subset], selectedSNPs, selectedGenes, chromosomes, prefix);
                                    break;
                            }

                            log.info("retVal: " + retVal)
                            log.info("====================")
                        }
                    }

                    if (writeClinicalData) {

                        //Grab the item from the data map that tells us whether we need the concept contexts.
                        Boolean includeConceptContext = jobDataMap.get("includeContexts", false);

                        //This is a list of concept codes that we use to filter the result instance id results.
                        String[] conceptCodeList = jobDataMap.get("concept_cds");

                        //This is list of concept codes that are parents to some child concepts. We need to expand these out in the service call.
                        List parentConceptCodeList = new ArrayList()

                        if (jobDataMap.get("parentNodeList", null) != null) {
                            //This variable tells us which variable actually holds the parent concept code.
                            String conceptVariable = jobDataMap.get("parentNodeList")

                            //Get the actual concept value from the map.
                            parentConceptCodeList.add(jobDataMap.get(conceptVariable))
                        } else {
                            parentConceptCodeList = []
                        }

                        //Make this blank instead of null if we don't find it.
                        if (conceptCodeList == null) conceptCodeList = []

                        //Set the flag that tells us whether or not to exclude the high level concepts. Should this logic even be in the DAO?
                        boolean filterHighLevelConcepts = false

                        //  String clinicalFilename = studyDir.absolutePath + '/Clinical/clinical_i2b2trans.txt'
                        // String key = subset.equalsIgnoreCase("subset1") ? typeNameClinical + "1" : typeNameClinical + "2";
                        //   resultFilesMap.put(key, clinicalFilename)


                        if (jobDataMap.get("analysis") == "DataExport") filterHighLevelConcepts = true
                        def platformsList = subsetSelectedPlatformsByFiles?.get(subset)?.get("MRNA.TXT")
                        //Reason for moving here: We'll get the map of SNP files from SnpDao to be output into Clinical file
                        def retVal = clinicalDataService.getData(studyList, studyDir, "clinical.i2b2trans", jobDataMap.get("jobName"),
                                resultInstanceIdMap[subset], conceptCodeList, selectedFilesList, pivotData, filterHighLevelConcepts,
                                snpFilesMap, subset, filesDoneMap, platformsList, parentConceptCodeList as String[], includeConceptContext)

                        if (jobDataMap.get("analysis") != "DataExport") {
                            //if i2b2Dao was not able to find data for any of the studies associated with the result instance ids, throw an exception.
                            if (!retVal) {
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
        if (providedSubsetName.indexOf("1") != -1 && providedSubsetName.indexOf("2") == -1) {
            return subset1;
        } else if (providedSubsetName.indexOf("1") == -1 && providedSubsetName.indexOf("2") != -1) {
            return subset2;
        } else {
            throw new Exception('Cannot define subset provided: can be only \'${subset1}\' or \'${subset2}\'. Provided subset name: \'${providedSubsetName}\'')
        }


        return null;
    }

    public static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if (files != null) { //some JVMs return null for empty dirs
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

    def download(String remoteUrl, String localUrl) {
        URL url = new URL(remoteUrl);
        BufferedReader inn = new BufferedReader(
                new InputStreamReader(url.openStream()))
        PrintWriter writer = new PrintWriter(localUrl, "UTF-8");

        String inputLine;

        while ((inputLine = inn.readLine()) != null) {
            writer.write(inputLine + System.getProperty("line.separator"));

        }
        writer.close();
        inn.close();
    }
}


