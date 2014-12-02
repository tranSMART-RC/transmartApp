package com.recomdata.transmart.data.export.omicsoftIntegration.util

import groovy.sql.Sql

import javax.sql.DataSource

/**
 * Created with IntelliJ IDEA.
 * User: transmart
 * Date: 12/13/13
 * Time: 1:48 AM
 * To change this template use File | Settings | File Templates.
 */
class SqlUtilsService {

	def dataSource

	Sql createSql (
	) throws Exception {
		Sql sql = new Sql(dataSource)
		if ( sql == null )
			throw new Exception("Error creating groovy.sql.Sql object from dataSource bean.");
		return sql;

	}

	String removeEscapes(
			String str
	) {
		String _str;
		_str = str.replace("\\_","_");
		_str = _str.replace("\\\\","\\");
		return _str;
	}

	String generateOmicsoftJobName() {
		def jobName = 'omicsoftExport_' + (new Date().format('yyyy_MM_dd__HH_mm_ss'))
		return jobName
	}

}
