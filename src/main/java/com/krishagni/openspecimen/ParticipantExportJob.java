package com.krishagni.openspecimen;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import com.krishagni.catissueplus.core.administrative.domain.ScheduledJobRun;
import com.krishagni.catissueplus.core.administrative.services.ScheduledTask;
import com.krishagni.catissueplus.core.biospecimen.domain.CollectionProtocolRegistration;
import com.krishagni.catissueplus.core.biospecimen.repository.CprListCriteria;
import com.krishagni.catissueplus.core.biospecimen.repository.DaoFactory;
import com.krishagni.catissueplus.core.common.PlusTransactional;
import com.krishagni.catissueplus.core.common.util.ConfigUtil;
import com.krishagni.catissueplus.core.common.util.CsvFileWriter;

@Configurable
public class ParticipantExportJob implements ScheduledTask{
	@Autowired
	private DaoFactory daoFactory;
	
	private static final Log logger = LogFactory.getLog(ParticipantExportJob.class);
	
	@Override
	public void doJob(ScheduledJobRun jobRun) throws IOException {
		
		try {
			exportParticipants();
			logger.error("Successfully created Participants.csv file with valid data");
		} catch (Exception e) {
			logger.error("Error operating inside class com.krishagni.catissueplus.core.biospecimen.services.impl.ParticipantExportJob");
		} 	
	}
	
	@PlusTransactional
	private void exportParticipants() throws IOException {
		List<Long> cpIds = daoFactory.getCollectionProtocolDao().getAllCpIds();
		
		CsvFileWriter csvFileWriter = getCSVWriter(getFile());		
		csvFileWriter.writeNext(getHeader());
		
		for (Long cpId : cpIds) {
    		CprListCriteria criteria = new CprListCriteria().cpId(cpId);

    		boolean endOfCpParticipants = false;
    		while (!endOfCpParticipants) {
      			List<CollectionProtocolRegistration> cprs = daoFactory.getCprDao().getCprs(criteria);  
      			exportToCsv(cprs, csvFileWriter);

      			criteria.startAt(cprs.size());
      			endOfCpParticipants = cprs.size() < criteria.maxResults();
      			
    		}
  		}
		
		csvFileWriter.flush();
		csvFileWriter.close();
	}
	
	private void exportToCsv(List<CollectionProtocolRegistration> cprs, CsvFileWriter csvFileWriter) {
		for(CollectionProtocolRegistration cprsDetail: cprs) {
		csvFileWriter.writeNext(getRow(cprsDetail));
		}
	}
		
	private CsvFileWriter getCSVWriter(OutputStream file) {
		return CsvFileWriter.createCsvFileWriter(file);
	}

	private OutputStream getFile() throws FileNotFoundException {
		try {
			return new FileOutputStream(ConfigUtil.getInstance().getDataDir() + File.separator + "Participants.csv");
		} catch (FileNotFoundException e) {
			logger.error("Error occured while creating file", e);
			throw new FileNotFoundException();
		}
	}
	
	private String[] getHeader() {
		return new String[] { "firstName", "middleName" };
	}

	private String[] getRow(CollectionProtocolRegistration cprDetail) {
		return new String[] {cprDetail.getParticipant().getFirstName(), cprDetail.getParticipant().getMiddleName()};
	}
}