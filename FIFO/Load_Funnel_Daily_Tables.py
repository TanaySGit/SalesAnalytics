import logging
import base64
from datetime import date, datetime, timedelta
import snowflake.connector
from FIFO import config
from timeit import default_timer as timer
from FIFO import snowflakeconnect

class loadfunnelailytables:
 ''' This class loads daily snapshot tables. ProcesssData method is called for loading the data. It checks the working day first.
 Some tables are loaded only on working day'''
 def __init__(self,batch):
   self.batch=batch
 def ProcessData(self):
  try:
   timerstep1=timer()
   today = datetime.now()

   logging.basicConfig(filename="FIFOout.log",format='%(asctime)s %(message)s',level=logging.INFO)
   logging.info("Daily load tables data processing started")

   WD=('WD02','WD05','WD10','WD15')

   year=today.year
   '''Get Current Month in MON format'''
   mon_column = today.strftime("%b").upper()
   periodname=mon_column+'-'+str(year)
   print(mon_column)
   print(periodname)

   snowflkconn = snowflakeconnect.snowflakeconnect()
   snowcursor = snowflkconn.createcur()



   # checkbatch= "SELECT NVL(MAX(BATCH_NUM),0) FROM RUN_TABLE; "
   # snowexec=snowcursor.execute(checkbatch)
   # getbatch=snowexec.fetchone()
   # getbatchnum= int(getbatch[0]) +1

   runtableinsert="INSERT INTO RUN_TABLE(BATCH_NUM,PROCESS,START_TIME,END_TIME,TIME_TAKEN_IN_MINT,STATUS_CD,PROCESS_DATE) VALUES( %s, 'Daily_Snapshot_Load',CURRENT_TIMESTAMP,null,null,'STARTED',CURRENT_TIMESTAMP);" %(self.batch)
   snowcursor.execute(runtableinsert)
   '''Use Current Month in select query to get WORKDAY'''

   checkwdstmnt= "select nvl(WORKDAY,'WE') FROM WD_CALENDER  WHERE %s =CURRENT_DATE; " %(mon_column)
   snowexec=snowcursor.execute(checkwdstmnt)
   getwd=snowexec.fetchone()

   if getwd is None:
    getwd='WD02'

   '''Get max number from the load sequence'''
   print("The workday for today is: "+str(getwd))
   logging.info("The workday for today is: "+str(getwd))




   if getwd[0]in WD:

      loadseq = [int(i["Load_Seq"]) for i in config.LOADLIST ]
      loadseq1 = max(loadseq)
      print("Today is a workday. Program will try to load below tables")
      logging.info("Today is a workday. Program will try to load below tables")
      tablelist=[i["Load_Table"] for i in config.LOADLIST ]
      print(tablelist)
      logging.info(tablelist)
      for i in loadseq:

       executestatement = [d for d in config.LOADLIST if int(d['Load_Seq']) == i ]
       gettablename=executestatement[0].get("Load_Table")
       executestatementstr=executestatement[0].get("Load_String")
       truncateoption = executestatement[0].get("Is_Truncate")
       executestatementstr=executestatementstr %(periodname,getwd[0])

       checkalreadyloaded=config.CHECK_IF_ALREADY_LOADED %(gettablename,periodname,getwd[0])
       checkdataexist=snowcursor.execute(checkalreadyloaded)
       checkdataexistressult=checkdataexist.rowcount

       if checkdataexistressult ==1:
        logging.info("The table " + gettablename + ' already loaded once today.So, not loading this time. To load the data for today again at first you have to delete todays data from the table.')
        print("The table " + gettablename + ' already loaded once today.So, not loading this time. To load the data for today again at first you have to delete todays data from the table.')
       else:
         if truncateoption == 'YES':
               print("Truncating the table " + gettablename + " as the tuncate option is set to yes for the table")
               logging.info("Truncating the table " + gettablename + " as the tuncate option is set to yes for the table")
               snowcursor.execute("TRUNCATE TABLE  " + gettablename)
         print("Loading the table " + gettablename)
         logging.info("Loading the table " + gettablename)
         timerstep2 = timer()
         snowexec = snowcursor.execute(executestatementstr)
         rowcount=snowexec.rowcount
         timerstep3 = timer()
         timedelta23=round((timerstep3-timerstep2)/60,2)
         snowcursor.execute("insert into RUN_TABLE_DETAILS(BATCH,TASK_PHASE,STATUS,TABLE_NAME,PROCESS_COUNT,ELAPSED_TIME_IN_MIN,PROCESS_DATE) VALUES ("+str(self.batch)+",'Daily_Snapshot_Load','Completed','"+gettablename+"','"+str(rowcount)+"','"+str(timedelta23)+"',CURRENT_TIMESTAMP);")
         print("Loading completed for table " + gettablename+". Took "+str(timedelta23)+" min and loaded # of rows "+str(rowcount))
         logging.info("Loading completed for table " + gettablename+". Took "+str(timedelta23)+" min and loaded # of rows "+str(rowcount))
   else:

    print("Today is a regular day(not a working day). Program will try to load below tables")
    logging.info("Today is a regular day(not a working day). Program will try to load below tables")
    tablelistreg = [i["Load_Table"] for i in config.LOADLIST if i["Load_Frequency"]=='DAILY']
    print(tablelistreg)
    logging.info(tablelistreg)
    loadseqreg=[int(i["Load_Seq"]) for i in config.LOADLIST if i["Load_Frequency"]=='DAILY' ]
    dailylist = [i for i in config.LOADLIST if i["Load_Frequency"] == 'DAILY']

    loadseq2 = max(loadseqreg)
    for i in loadseqreg:

     executestatement = [d for d in dailylist if int(d['Load_Seq']) == i ]
     gettablename = executestatement[0].get("Load_Table")
     truncateoption=executestatement[0].get("Is_Truncate")
     executestatementstr = executestatement[0].get("Load_String")
     executestatementstr = executestatementstr % (periodname, getwd[0])

     checkalreadyloaded = config.CHECK_IF_ALREADY_LOADED % (gettablename, periodname, getwd[0])

     checkdataexist = snowcursor.execute(checkalreadyloaded)
     checkdataexistressult = checkdataexist.rowcount

     if checkdataexistressult == 1:
      print("The table " + gettablename + ' already loaded once today.So, not loading this time. To load the data for today again at first you have to delete todays data from the table.')
      logging.info("The table " + gettablename + ' already loaded once today.So, not loading this time. To load the data for today again at first you have to delete todays data from the table.')
     else:


      if truncateoption=='YES':
       print("Truncating the table "+gettablename+ " as the tuncate option is set to yes for the table")
       logging.info("Truncating the table "+gettablename+ " as the tuncate option is set to yes for the table")
       snowcursor.execute("TRUNCATE TABLE  "+gettablename)
    print("Loading table "+gettablename)
    logging.info("Loading table "+gettablename)
    timerstep4 = timer()
    snowexec = snowcursor.execute(executestatementstr)
    rowcount=snowexec.rowcount
    timerstep5 = timer()
    timedelta23 = round((timerstep5 - timerstep4) / 60, 2)
    snowcursor.execute("insert into RUN_TABLE_DETAILS(BATCH,TASK_PHASE,STATUS,TABLE_NAME,PROCESS_COUNT,ELAPSED_TIME_IN_MIN,PROCESS_DATE) VALUES (" + str(self.batch) + ",'Daily_Snapshot_Load','Completed','" + gettablename + "','" + str(rowcount) + "','" + str(timedelta23) + "',CURRENT_TIMESTAMP);")
    print("Loading completed for table " + gettablename+". Took "+str(timedelta23)+" min and loaded # of rows "+str(rowcount))
    logging.info("Loading completed for table " + gettablename+". Took "+str(timedelta23)+" min and loaded # of rows "+str(rowcount))
    updaterun="UPDATE  RUN_TABLE SET END_TIME=CURRENT_TIMESTAMP , TIME_TAKEN_IN_MINT= datediff(minute, START_TIME, CURRENT_TIMESTAMP) WHERE BATCH_NUM=%s" %(self.batch)
    snowcursor.execute(checkalreadyloaded)

   updaterun="UPDATE  RUN_TABLE SET END_TIME=CURRENT_TIMESTAMP , TIME_TAKEN_IN_MINT= datediff(minute, START_TIME, CURRENT_TIMESTAMP),STATUS_CD='COMPLETED' WHERE BATCH_NUM=%s" %(self.batch)
   snowcursor.execute(updaterun)

   snowcursor.close()

   print("Daily load tables data processing completed. Closing the snowflake connection")
   logging.info("Daily load tables data Processing completed. Closing the snowflake connection")

   elapsed_time = timer() - timerstep1
   print("Total time taken for the load in minute: " + str(round(elapsed_time/60,2)))
   logging.info("Total time taken for the load in minute: " +str(round(elapsed_time/60,2)))
   return(200)
  except Exception as e:
   updaterun="UPDATE  RUN_TABLE SET END_TIME=CURRENT_TIMESTAMP , TIME_TAKEN_IN_MINT= datediff(minute, START_TIME, CURRENT_TIMESTAMP),STATUS_CD='FAILED' WHERE BATCH_NUM=%s" %(self.batch)
   snowcursor.execute(updaterun)
   print(e)
   logging.error(e)
   return 400

  finally:
   snowcursor.close()
#loadffodaily=loadfunnelailytables(45)
#a=loadffodaily.ProcessData()
#print(a)
