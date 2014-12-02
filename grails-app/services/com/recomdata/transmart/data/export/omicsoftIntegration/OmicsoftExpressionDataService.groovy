package com.recomdata.transmart.data.export.omicsoftIntegration

import com.recomdata.transmart.data.export.omicsoftIntegration.util.SqlUtilsService
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.StringUtils
import org.codehaus.groovy.grails.commons.ConfigurationHolder
import org.codehaus.groovy.grails.commons.GrailsApplication
import org.springframework.transaction.annotation.Transactional

import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

class OmicsoftExpressionDataService {

    private static final String PROBE_ID = "PROBE ID"
    def dataSource;
    def config = ConfigurationHolder.config;
	SqlUtilsService sqlUtilsService
	GrailsApplication grailsApplication

    final String convertedExpressionDataDefaultFilename = 'biomarker.expression.txt';
    final String tempDir = config.com.recomdata.plugins.tempFolderDirectory;

    /**
     * Load expression data from db by resultInstanceId
     * @param resultInstanceId
     * @param subsetId
     * @return
     */
    @Transactional(readOnly = true)
    def exportExpressionData(
            resultInstanceId,
            subsetId
    ) {
		def jobName = sqlUtilsService.generateOmicsoftJobName()

        // Create objects we use to form JDBC connection.
        def con, stmt, stmt1, rs = null;

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
                ssm.GPL_ID
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

        // Prepare the SQL statement.
        stmt = con.prepareStatement(sqlQuery);
        stmt.setString(1, resultInstanceId);
        stmt.setFetchSize(fetchSize)

        // Sample query
        stmt1 = con.prepareStatement(sampleQuery);
        stmt1.setString(1, resultInstanceId);
        stmt1.setFetchSize(fetchSize);

        def sampleType, sampleCD, assayID, GPL_ID, logIntensity, probeId, prevProbeId = null;
        def char separator = '\t';

        // Load header data
        log.info("Start header data retrieving query");
        log.debug("Header query : " + sqlQuery);
        rs = stmt.executeQuery();

        //def headerMap = [:];
        StringBuilder headerString = new StringBuilder();
        headerString.append(PROBE_ID).append(separator);
        StringBuilder tmpString = new StringBuilder();

        try {
            while (rs.next()) {
                tmpString = new StringBuilder();

                sampleType = rs.getString("SAMPLE_TYPE");
                sampleCD = rs.getString("SAMPLE_CD");
                assayID = rs.getString("ASSAY_ID");
                GPL_ID = rs.getString("GPL_ID");

                tmpString = StringUtils.isNotEmpty(sampleCD) && StringUtils.isNotEmpty(sampleType) && StringUtils.isNotEmpty(GPL_ID) ?
                    tmpString.append(sampleCD).append("_").append(sampleType).append("_").append(GPL_ID) : null;

                if (assayID != null && tmpString != null) {
                    //headerMap.put(assayID, tmpString);
                    headerString.append(tmpString).append(separator);
                }
            }
        } finally {
            rs?.close();
            stmt?.close();
        }
        log.info("Finished header data retrieving query");

        if (!headerString.toString().trim().equals(PROBE_ID)) {
            // Create export file
            String fileSeparator = File.separator;
            String folderName = "omicsoftExport_expression_" + new Date().format('yyyy_MM_dd__HH_mm_ss') + fileSeparator + "subset" + subsetId + fileSeparator;
            // Check folder
            File subsetFolder = new File(tempDir + folderName);
            createDir(subsetFolder);

            File exportFile = new File(tempDir + folderName + convertedExpressionDataDefaultFilename);
            BufferedWriter bw = new BufferedWriter(new FileWriter(exportFile.getAbsolutePath()));
            def convertedHeaderString = DataConvertors.convertExpressionHeader(headerString.toString())
            bw.writeLine(convertedHeaderString);

            // Load sample data
            log.info("Start sample retrieving query");
            log.debug("Sample Query : " + sampleQuery);
            rs = stmt1.executeQuery();

            tmpString = new StringBuilder();
            try {
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
                }
            } finally {
                bw.writeLine(prevProbeId + separator + tmpString.toString());
                bw.flush();
                bw.close();
                rs?.close();
                stmt1?.close();
            }

            log.info("Finished sample retrieving query");
            //return exportFile.getAbsolutePath();

			// zip data
			String zipFilename = jobName + '.zip'
			String dataToZip = exportFile.getAbsolutePath()
			try {
			//	def tmpDir = grailsApplication.config.com.recomdata.plugins.tempFolderDirectory
                def tmpDir =  grailsApplication.config.com.recomdata.plugins.tempFolderDirectory
				FileOutputStream fos = new FileOutputStream( tmpDir + File.separator + zipFilename )
				BufferedOutputStream bos = new BufferedOutputStream(fos)
				ZipOutputStream zippedData = new ZipOutputStream(bos)

				FileInputStream data = new FileInputStream(dataToZip)
				ZipEntry ze = new ZipEntry(convertedExpressionDataDefaultFilename)
				zippedData.putNextEntry(ze)
				zippedData << data
				zippedData.flush()
				zippedData.close()
			} catch (Exception e) {
				throw e
			}
			return jobName
        } else {
            return null;
        }
    }

    private void createDir(
            final File dir
    ) {
        if (!dir.isDirectory())
            dir.mkdirs();
    } // private void createDir(
}