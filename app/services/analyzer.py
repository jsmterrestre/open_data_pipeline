import pandas as pd
import numpy as np
from typing import Dict, List, Any
from collections import defaultdict

class ConcentrationAnalyzer:
    def __init__(self):
        self.numeric_columns = []
        self.categorical_columns = []

    def analyze_concentration(self, df: pd.DataFrame, top_n: int = 10) -> Dict[str, Any]:
        """
        Analyze concentration patterns in the data.
        Returns concentration metrics for both numeric and categorical columns.
        """
        self._detect_column_types(df)
        
        results = {
            "numeric_concentration": self._analyze_numeric_concentration(df, top_n),
            "categorical_concentration": self._analyze_categorical_concentration(df, top_n),
            "summary": self._generate_summary(df)
        }
        
        return results

    def _detect_column_types(self, df: pd.DataFrame):
        """Detect numeric and categorical columns."""
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                self.numeric_columns.append(col)
            else:
                self.categorical_columns.append(col)

    def _analyze_numeric_concentration(self, df: pd.DataFrame, top_n: int) -> Dict[str, Any]:
        """Analyze concentration in numeric columns."""
        results = {}
        
        for col in self.numeric_columns:
            # Calculate basic statistics
            stats = df[col].describe()
            
            # Calculate concentration metrics
            total = df[col].sum()
            sorted_values = df[col].sort_values(ascending=False)
            
            # Calculate top N concentration
            top_n_sum = sorted_values.head(top_n).sum()
            top_n_percentage = (top_n_sum / total) * 100
            
            # Calculate Gini coefficient
            gini = self._calculate_gini(sorted_values)
            
            results[col] = {
                "statistics": {
                    "mean": stats["mean"],
                    "std": stats["std"],
                    "min": stats["min"],
                    "max": stats["max"],
                    "median": stats["50%"]
                },
                "concentration": {
                    f"top_{top_n}_percentage": top_n_percentage,
                    "gini_coefficient": gini
                }
            }
        
        return results

    def _analyze_categorical_concentration(self, df: pd.DataFrame, top_n: int) -> Dict[str, Any]:
        """Analyze concentration in categorical columns."""
        results = {}
        
        for col in self.categorical_columns:
            # Calculate value counts
            value_counts = df[col].value_counts()
            total = len(df)
            
            # Calculate top N concentration
            top_n_counts = value_counts.head(top_n)
            top_n_percentage = (top_n_counts.sum() / total) * 100
            
            # Calculate Herfindahl-Hirschman Index (HHI)
            hhi = self._calculate_hhi(value_counts, total)
            
            results[col] = {
                "top_values": {
                    value: {
                        "count": count,
                        "percentage": (count / total) * 100
                    }
                    for value, count in top_n_counts.items()
                },
                "concentration": {
                    f"top_{top_n}_percentage": top_n_percentage,
                    "hhi": hhi
                }
            }
        
        return results

    def _calculate_gini(self, values: pd.Series) -> float:
        """Calculate Gini coefficient for numeric concentration."""
        values = values.sort_values()
        n = len(values)
        index = np.arange(1, n + 1)
        return ((2 * index - n - 1) * values).sum() / (n * values.sum())

    def _calculate_hhi(self, value_counts: pd.Series, total: int) -> float:
        """Calculate Herfindahl-Hirschman Index for categorical concentration."""
        market_shares = value_counts / total
        return (market_shares ** 2).sum()

    def _generate_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate overall summary statistics."""
        return {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "numeric_columns": len(self.numeric_columns),
            "categorical_columns": len(self.categorical_columns),
            "missing_values": df.isnull().sum().to_dict()
        } 