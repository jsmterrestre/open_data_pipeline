import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

class TransformationLogger:
    def __init__(self):
        self.logger = logging.getLogger('transformation_logger')
        self.logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        log_dir = Path('data/logs')
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up file handler
        log_file = log_dir / f'transformations_{datetime.now().strftime("%Y%m%d")}.log'
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Set up formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        if not self.logger.handlers:
            self.logger.addHandler(file_handler)

    def log_transformation(self, 
                          operation: str,
                          input_data: Dict[str, Any],
                          output_data: Dict[str, Any],
                          metadata: Dict[str, Any] = None):
        """
        Log a data transformation operation.
        
        Args:
            operation: Name of the transformation operation
            input_data: Input data metadata
            output_data: Output data metadata
            metadata: Additional metadata about the transformation
        """
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'input': input_data,
            'output': output_data,
            'metadata': metadata or {}
        }
        
        self.logger.info(json.dumps(log_entry))

    def log_analysis(self,
                    analysis_type: str,
                    parameters: Dict[str, Any],
                    results: Dict[str, Any]):
        """
        Log an analysis operation.
        
        Args:
            analysis_type: Type of analysis performed
            parameters: Parameters used in the analysis
            results: Results of the analysis
        """
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'analysis_type': analysis_type,
            'parameters': parameters,
            'results': results
        }
        
        self.logger.info(json.dumps(log_entry))

    def get_transformation_history(self, filename: str) -> list:
        """
        Get the transformation history for a specific file.
        
        Args:
            filename: Name of the file to get history for
            
        Returns:
            List of transformation logs for the file
        """
        history = []
        log_dir = Path('data/logs')
        
        for log_file in log_dir.glob('transformations_*.log'):
            with open(log_file, 'r') as f:
                for line in f:
                    try:
                        log_entry = json.loads(line.split(' - ')[-1])
                        if (log_entry.get('input', {}).get('filename') == filename or
                            log_entry.get('output', {}).get('filename') == filename):
                            history.append(log_entry)
                    except:
                        continue
        
        return history 