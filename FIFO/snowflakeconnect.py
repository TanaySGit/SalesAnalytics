import snowflake.connector
from FIFO import config
from META import db
import logging
class snowflakeconnect:
    def __init__(self):
        pass

    def createcur(self):

        logging.basicConfig(filename="FIFOout.log", format='%(asctime)s %(message)s', level=logging.INFO)
        try:
         self.connectsnow = snowflake.connector.connect( user=db.user,
                                       password=db.password,
                                       account=db.account,
                                       database=db.database,
                                       schema=db.schema,
                                       warehouse=db.warehouse,
                                       role=db.role
                                      )

         self.snowcur=self.connectsnow.cursor()
         return self.snowcur
        except Exception as e:
            print("Snowflake Connection Issue. Holy guess is password expired")
            logging.info("Snowflake Connection Issue. Holy guess is password expired")
            logging.error(e)

