'''
Data ingestion is VERY important since data is very important
We may work with different data sources like mongoDB....xxx
So we need this module to includes different types of functions
to extract the data from different kinds of sources.

This python file will read the data from the sources and split them into training and testing dataset before performing data transformation
'''

import os
import sys
sys.path.insert(0, 'Z:\Projects\mlproject\src')
from exception import CustomException
from logger import logging
import pandas as pd

from sklearn.model_selection import train_test_split
from dataclasses import dataclass # use to create class variable

from components.data_transformation import DataTransformation, DataTransformationConfig

'''
before performing data ingestion, there should be some inputs variable to store the data read from the sources
The inputs variable will be saved in class DataIngestionConfig
'''

@dataclass # only holds data values
class DataIngestionConfig:  # knows where to save the data
    train_data_path: str=os.path.join('artifact', "train.csv")
    test_data_path: str=os.path.join('artifact', "test.csv")
    raw_data_path: str=os.path.join('artifact', "raw.csv")


class DataIngestion:
    def __init__(self):  # constructor to call the dataingestionconfig class and store as ingestion_config object
        self.ingestion_config = DataIngestionConfig()

    def initiate_data_ingestion(self): # read data from different database
        logging.info("Entered the data ingestion method or component")
        try:
            df=pd.read_csv('notebook\data\stud.csv')  # change to mongoDB/MySQL later
            logging.info('Read the dataset as dataframe')

            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path),exist_ok=True) # create the folder and if exist then ok.

            df.to_csv(self.ingestion_config.raw_data_path,index=False,header=True)

            logging.info("Train test split initiated")
            train_set,test_set=train_test_split(df,test_size=0.2,random_state=42)

            train_set.to_csv(self.ingestion_config.train_data_path,index=False,header=True)
            test_set.to_csv(self.ingestion_config.test_data_path,index=False,header=True)

            logging.info("Ingestion of the data is completed")
            print(self.ingestion_config.train_data_path)
            return(
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path
            )
        except Exception as e:
            raise CustomException(e,sys)

if __name__=="__main__":
    obj=DataIngestion()
    train_path, test_path = obj.initiate_data_ingestion()
    logging.info(f"train path: {train_path}, \n test path: {test_path}")
    data_transformation = DataTransformation()
    data_transformation.initiate_data_transformation(train_path, test_path)