CREATE OR REPLACE TABLE METADATA.JOB.JOB_HISTORY
(
	DATASET_ID TEXT, 					                
	JOB_ID TEXT,  						                 
    	JOB_NAME TEXT,                                   
    	DESCRIPTION TEXT,                                  
    	PRE_JOB_MAX_TABLE_COUNT NUMBER,					   
    	START_TIME TIMESTAMP,  				             
	END_TIME TIMESTAMP, 					          
	ELAPSED_TIME TIMESTAMP,						    
	OFFSET_START NUMBER,					           
	OFFSET_END NUMBER, 					          
	POST_JOB_MAX_TABLE_COUNT NUMBER,				 
	JOB_STATUS TEXT 						            
);


