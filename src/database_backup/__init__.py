
import importlib.metadata 
import logging
database_backup_logger = logging.getLogger(__name__)

__version__ =  importlib.metadata.version('database_backup') 

