import pandas as pd
import numpy as np
from typing import Dict, List
import re
from app.services.logger import TransformationLogger

class DataNormalizer:
    def __init__(self):
        self.column_mappings = {}
        self.numeric_columns = []
        self.categorical_columns = []
        self.date_columns = []
        self.logger = TransformationLogger()

    def normalize(self, df: pd.DataFrame, filename: str = None) -> pd.DataFrame:
        """
        Normalize the input DataFrame by:
        1. Cleaning column names
        2. Converting data types
        3. Handling missing values
        4. Standardizing formats
        """
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Log input state
        input_metadata = {
            'filename': filename,
            'shape': df.shape,
            'columns': df.columns.tolist(),
            'dtypes': df.dtypes.astype(str).to_dict(),
            'missing_values': df.isnull().sum().to_dict()
        }
        
        # Clean column names
        df.columns = self._clean_column_names(df.columns)
        
        # Detect column types
        self._detect_column_types(df)
        
        # Convert data types
        df = self._convert_data_types(df)
        
        # Handle missing values
        df = self._handle_missing_values(df)
        
        # Standardize formats
        df = self._standardize_formats(df)
        
        # Log output state
        output_metadata = {
            'filename': filename,
            'shape': df.shape,
            'columns': df.columns.tolist(),
            'dtypes': df.dtypes.astype(str).to_dict(),
            'missing_values': df.isnull().sum().to_dict(),
            'column_types': {
                'numeric': self.numeric_columns,
                'categorical': self.categorical_columns,
                'date': self.date_columns
            }
        }
        
        # Log the transformation
        self.logger.log_transformation(
            operation='normalize',
            input_data=input_metadata,
            output_data=output_metadata,
            metadata={
                'column_mappings': self.column_mappings,
                'transformations_applied': [
                    'column_name_cleaning',
                    'data_type_conversion',
                    'missing_value_handling',
                    'format_standardization'
                ]
            }
        )
        
        return df

    def _clean_column_names(self, columns: List[str]) -> List[str]:
        """Clean column names by removing special characters and standardizing format."""
        cleaned = []
        for col in columns:
            # Convert to string if not already
            col = str(col)
            
            # Remove special characters and spaces
            col = re.sub(r'[^a-zA-Z0-9]', '_', col)
            
            # Convert to lowercase
            col = col.lower()
            
            # Remove leading/trailing underscores
            col = col.strip('_')
            
            # Replace multiple underscores with single underscore
            col = re.sub(r'_+', '_', col)
            
            # Store original to cleaned mapping
            self.column_mappings[col] = str(columns[len(cleaned)])
            
            cleaned.append(col)
        return cleaned

    def _detect_column_types(self, df: pd.DataFrame):
        """Detect and store column types for later processing."""
        for col in df.columns:
            # Check if column is numeric
            if pd.api.types.is_numeric_dtype(df[col]):
                self.numeric_columns.append(col)
            
            # Check if column is categorical
            elif df[col].nunique() < len(df) * 0.5:  # Less than 50% unique values
                self.categorical_columns.append(col)
            
            # Check if column is date
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                self.date_columns.append(col)
            else:
                # Try to convert to datetime
                try:
                    pd.to_datetime(df[col])
                    self.date_columns.append(col)
                except:
                    pass

    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert columns to appropriate data types."""
        # Convert numeric columns
        for col in self.numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert date columns
        for col in self.date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert categorical columns
        for col in self.categorical_columns:
            df[col] = df[col].astype('category')
        
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values based on column type."""
        # For numeric columns, fill with median
        for col in self.numeric_columns:
            df[col] = df[col].fillna(df[col].median())
        
        # For categorical columns, fill with mode
        for col in self.categorical_columns:
            df[col] = df[col].fillna(df[col].mode()[0])
        
        # For date columns, fill with forward fill
        for col in self.date_columns:
            df[col] = df[col].fillna(method='ffill')
        
        return df

    def _standardize_formats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize formats for different column types."""
        # Standardize numeric columns to float
        for col in self.numeric_columns:
            df[col] = df[col].astype(float)
        
        # Standardize categorical columns to string
        for col in self.categorical_columns:
            df[col] = df[col].astype(str)
        
        # Standardize date columns to datetime
        for col in self.date_columns:
            df[col] = pd.to_datetime(df[col])
        
        return df 