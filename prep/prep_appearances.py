import pandas as pd
from lib.appearances_processor import AppearancesProcessor

processor = AppearancesProcessor(raw_file_path='../data/raw/appearances.json')

processor.process()
processor.validate()
processor.export()
