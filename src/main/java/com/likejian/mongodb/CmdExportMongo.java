package com.likejian.mongodb;

import java.io.InputStream;

public class CmdExportMongo {
	
	public static void main(String[] args) {
		CmdExportMongo cmdExportMongo = new CmdExportMongo();
		String collectionName = "collection201306";
		String csvName = "collection201306";
		String exportFileName = "col_0,col_1,col_2,col_3";
		cmdExportMongo.export2Csv(collectionName, csvName, exportFileName);
	}
	
	private void export2Csv(String collectionName, String csvName, String exportFileName){
		Runtime run = Runtime.getRuntime();   
	    try {   
	        Process process = run.exec("cmd /c mongoexport -d test -c "+ collectionName +" --csv -f "+ exportFileName +" -o d:/data/"+ csvName +".csv");   
	        InputStream in = process.getInputStream();     
	        while (in.read() != -1) {   
	            System.out.println(in. read());   
	        }   
	        in.close();   
	        process.waitFor();   
	    } catch (Exception e) {            
	        e.printStackTrace();   
	    }  
	}
}
