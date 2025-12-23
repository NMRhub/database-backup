
import importlib.metadata 
import logging

__version__ =  importlib.metadata.version('database_backup') 

class ActionLogger(logging.Logger):
    """Add 'action' logging level"""
    # Between INFO and WARNING
    setattr(logging, 'ACTION', 25)
    logging.addLevelName(25, 'ACTION')

    def action(self, *args, **kwargs):
        self.log(25, *args, **kwargs)

logging.setLoggerClass(ActionLogger)

database_backup_logger : ActionLogger = logging.getLogger(__name__)


