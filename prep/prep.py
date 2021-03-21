import pandas as pd
from lib.processor import Processor

processor = Processor()

# processor.process_assets()
processor.generate_datapackage()
print(processor.validation_report)
