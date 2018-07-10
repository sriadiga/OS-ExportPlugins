package com.krishagni.openspecimen;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
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
			//Sri: Again you are swallowing exceptions. have given this comment multiple times. Never swallow exceptions.
			logger.error("Error operating inside class com.krishagni.catissueplus.core.biospecimen.services.impl.ParticipantExportJob");
			//Sri: You are writing an meaningless error message which is of no use to anyone.
			//Sri: Log the exception msg.
		} 	
	}
	
	@PlusTransactional
	private void exportParticipants() throws Exception {
		List<Long> cpIds = daoFactory.getCollectionProtocolDao().getAllCpIds();
		
		CsvFileWriter csvFileWriter = getCSVWriter(getFile());		
		csvFileWriter.writeNext(getHeader());
		
		for (Long cpId : cpIds) {
    		CprListCriteria criteria = new CprListCriteria().cpId(cpId);
    		boolean endOfCpParticipants = false;
   
    		    while (!endOfCpParticipants) {
      			  List<CollectionProtocolRegistration> cprs = daoFactory.getCprDao().getCprs(criteria);  
      			  exportToCsv(cprs, csvFileWriter);
      			  csvFileWriter.flush();
      			  criteria.startAt(cprs.size());
      			  endOfCpParticipants = cprs.size() < criteria.maxResults();
    		    }
  		}
		csvFileWriter.close();
	}
	
	private void exportToCsv(List<CollectionProtocolRegistration> cprs, CsvFileWriter csvFileWriter) {
		for(CollectionProtocolRegistration cprDetail : cprs) {
			csvFileWriter.writeNext(getRow(cprDetail));
		}
	}
		
	private CsvFileWriter getCSVWriter(OutputStream file) {
		return CsvFileWriter.createCsvFileWriter(file);
	}

	private OutputStream getFile() throws Exception {
		String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		
		try {
			return new FileOutputStream(ConfigUtil.getInstance().getDataDir() + File.separator + "Participants_" + timeStamp + ".csv", false);
		} catch (Exception e) {
			logger.error("Error occured while creating file");
			//Sri: Another meaningless error message. Just sit for a min, close your eyes and think 
			// you are some random developer after 2 years seeing this msg in the log.
			// And think whether he will understand anything from it.
			throw new Exception();
		}
	}
	
	private String[] getHeader() {
		return new String[] {"firstName", "lastName"};
	}

	private String[] getRow(CollectionProtocolRegistration cprDetail) {
		return new String[] {cprDetail.getParticipant().getFirstName(), cprDetail.getParticipant().getLastName()};
	}
}
