import pymrio
from pathlib import Path
dataPath = Path(__file__).parent
exio3 = pymrio.download_exiobase3(storage_folder=dataPath,system='pxp',years=[2022])
