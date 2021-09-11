# Prototype version of Pipeline module

import logging
from subprocess import Popen
import time

# Start timer to record Pipeline running time
start_time_pipeline = time.time()

# Logging setup
class HandlerFilter():
    '''Class to filter handler based on message levels.'''
    def __init__(self, level):
        '''
        Initialize HandleFilter object.
              
        Args:
            level: Level to filter handler with
        '''
        self.__level = level

    def filter(self, log_record):
        '''
        Filter log record based on level.
              
        Args:
            log_record: Log to filter
        '''

        return log_record.levelno == self.__level

# Logger setup (emit log records)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Handler setup (send the log records to the appropriate destination)
console_handler = logging.StreamHandler()
logger.addHandler(console_handler)

file_handler = logging.FileHandler("C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Open-ended_Capstone\\data\\logs\\pipeline.log")
logger.addHandler(file_handler)

# Filter setup (based on the message level)
console_handler.addFilter(HandlerFilter(logging.INFO))
# file_handler.addFilter(HandlerFilter(logging.WARNING))

# Formatter setup (specify the layout of log records in the final output)
formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s: %(message)s')
file_handler.setFormatter(formatter)

# p = Popen([r'C:\secondaryScript1.py', "ArcView"], shell=True, stdin=PIPE, stdout=PIPE)


# Start timer to record Script 1 running time
start_time_s1 = time.time()

extraction = Popen(["python", "C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Open-ended_Capstone\\modules\\data_extraction.py"])
extraction.wait()

# Record Script 1 running time
script_01_time = round(time.time() - start_time_s1, 2)


if extraction.returncode == 0:
    logger.info(f"'data_extraction' script was successfully executed. Runnig time was {script_01_time} secs")

    # Start timer to record Script 2 running time
    start_time_s2 = time.time()
    
    cleaning = Popen(["python", "C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Open-ended_Capstone\\modules\\data_cleaning.py"])
    cleaning.wait()

    # Record Script 2 running time
    script_02_time = round(time.time() - start_time_s2, 2)

    if cleaning.returncode == 0:

        logger.info(f"'data_cleaning' script was successfully executed. Runnig time was {script_02_time} secs")

        # Start timer to record Script 2 running time
        start_time_s3 = time.time()
        
        uploading = Popen(["python", "C:\\Users\\FBLServer\\Documents\\PythonScripts\\SB\\Open-ended_Capstone\\modules\\data_uploading.py"])
        uploading.wait()

        # Record Script 3 running time
        script_03_time = round(time.time() - start_time_s3, 2)

        if uploading.returncode == 0:
            logger.info(f"'data_uploading' script was successfully executed. Runnig time was {script_03_time} secs")
            
            pipeline_time = round(time.time() - start_time_pipeline, 2)
            logger.info(f"'pipeline' script was successfully executed. Runnig time was {pipeline_time} secs")
        
        else:
            logger.error(f"'data_uploading' script was NOT successfully executed. Runnig time was {script_03_time} secs")

    else:
        logger.error(f"'data_cleaning' script was NOT successfully executed. Runnig time was {script_02_time} secs")


else:
    logger.error(f"'data_extraction' script was NOT successfully executed. Runnig time was {script_01_time} secs")