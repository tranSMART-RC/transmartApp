package com.recomdata.transmart.data.export.omicsoftIntegration

import org.apache.commons.io.FilenameUtils

/**
 * User: Leonid.Romanovskii
 * Date: 12/16/13
 */
class DataConvertors {

	/**
	 * Converts clinical file, exported by transmart, to omnisoft format
	 * Returns filename of converted data
	 * @param filename
	 * @return
	 */
	public static String convertMrnaToOmnisoft(
		final String filename,
		final String targetFolder
	) throws Exception {
		String basename = FilenameUtils.getBaseName(filename)
		String ext = FilenameUtils.getExtension(filename)
		String convertedDataFilename =
			targetFolder + File.separator +
					basename + "_omnisoft." + ext
		;
		BufferedWriter bw = new BufferedWriter( new FileWriter( convertedDataFilename ) )

		BufferedReader br = new BufferedReader( new FileReader(filename) )
		String headerLine = convertExpressionHeader( br.readLine() ) // reading and converting header line
		bw.writeLine( headerLine )

		// all data lines are just rewritten
		String line
		while ( ( line = br.readLine() ) != null ) {
			bw.writeLine( line )
		}

		bw.close()
		br.close()
		return convertedDataFilename

	}

	/**
	 * Converts mrna file, exported by transmart, to omnisoft format.
	 * Returns filename of converted data
	 * @param filename
	 * @return
	 */
	public static String convertClinicalToOmnisoft(
			final String _filename,
			final String targetFilename
	) throws Exception {
		String convertedDataFilename = targetFilename
		try {
		def filename = _filename + '.txt'
		//String basename = FilenameUtils.getBaseName(filename)
		//String ext = FilenameUtils.getExtension(filename)

			//targetFolder + File.separator +
			//basename + "_omnisoft." + ext
		;
		BufferedWriter bw = new BufferedWriter( new FileWriter( convertedDataFilename ) )

		BufferedReader br = new BufferedReader( new FileReader(filename) )
		String headerLine = convertClinicalHeader( br.readLine() ) // reading and converting header line
		bw.writeLine( headerLine )
        bw.writeLine( headerLine )

		// all data lines are just rewritten
		String line
		while ( ( line = br.readLine() ) != null ) {
			bw.writeLine( line )
		}

		bw.close()
		br.close()
		} catch (Exception e) {
			e.printStackTrace()
			throw e
		}
		return convertedDataFilename
	}

	private static String convertClinicalHeader(
	    final String headerLine
	) throws Exception {
		String[] l = headerLine.split("\t")

		if ( l[0] != 'PATIENT.ID' )
			println "First column name of exported by transmart clinical file is not PATIENT.ID, as supposed. Something is going wrong, continuing export."
		l[0] = 'ID'
		try {
		for ( int colNum = 1; colNum < l.length; colNum++ ) {
			// get data label as column name
			String colName = l[colNum];
			String[] c = colName.split("\\\\");
			l[colNum] = c[c.length - 1]
		}
		} catch( Exception e ){
			e.printStackTrace()
		}
		return l.join("\t");
	} // private static String convertClinicalHeader

	public static String convertExpressionHeader(
			final String headerLine
	) throws Exception {
		String[] l = headerLine.split("\t")
		l[0] = 'PROBE_ID'
		for ( int colNum = 1; colNum < l.length; colNum++ ) {
			// get data label as column name
			String colName = l[colNum];
			String[] c = colName.split("_");
			l[colNum] = c[0]
		}
		return l.join("\t");
	} // private static String convertExpressionHeader
}
