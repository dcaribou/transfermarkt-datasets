import pandas as pd
from lib.clubs_processor import ClubsProcessor

processor = ClubsProcessor(raw_file_path='../data/raw/clubs.json')

processor.process()
processor.validate()
processor.export()
