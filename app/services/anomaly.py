import pandas as pd
import numpy as np
from typing import Dict, List, Any
from pyod.models.knn import KNN
from pyod.models.iforest import IForest
from sklearn.preprocessing import StandardScaler

class AnomalyDetector:
    def __init__(self):
        self.numeric_columns = []
        self.models = {}
        self.scaler = StandardScaler()

    def detect_anomalies(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Detect anomalies in the data using multiple methods.
        Returns anomaly scores and detected outliers.
        """
        self._detect_numeric_columns(df)
        
        if not self.numeric_columns:
            return {"error": "No numeric columns found for anomaly detection"}
        
        # Prepare numeric data
        numeric_data = df[self.numeric_columns].copy()
        
        # Scale the data
        scaled_data = self.scaler.fit_transform(numeric_data)
        
        # Initialize results dictionary
        results = {
            "anomaly_scores": {},
            "detected_anomalies": {},
            "summary": {}
        }
        
        # Apply different anomaly detection methods
        methods = {
            "knn": KNN(contamination=0.1),
            "isolation_forest": IForest(contamination=0.1, random_state=42)
        }
        
        for method_name, model in methods.items():
            # Fit and predict
            model.fit(scaled_data)
            scores = model.decision_scores_
            
            # Store results
            results["anomaly_scores"][method_name] = {
                col: scores.tolist() for col in self.numeric_columns
            }
            
            # Detect anomalies (scores above threshold)
            threshold = np.percentile(scores, 90)  # Top 10% as anomalies
            anomalies = scores > threshold
            
            # Store detected anomalies
            results["detected_anomalies"][method_name] = {
                "indices": np.where(anomalies)[0].tolist(),
                "count": int(anomalies.sum()),
                "percentage": float(anomalies.mean() * 100)
            }
        
        # Generate summary statistics
        results["summary"] = self._generate_summary(df, results)
        
        return results

    def _detect_numeric_columns(self, df: pd.DataFrame):
        """Detect numeric columns in the DataFrame."""
        self.numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()

    def _generate_summary(self, df: pd.DataFrame, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary statistics for anomaly detection results."""
        summary = {
            "total_rows": len(df),
            "numeric_columns_analyzed": len(self.numeric_columns),
            "methods_applied": list(results["anomaly_scores"].keys())
        }
        
        # Add method-specific summaries
        for method in results["detected_anomalies"]:
            summary[f"{method}_anomalies"] = {
                "count": results["detected_anomalies"][method]["count"],
                "percentage": results["detected_anomalies"][method]["percentage"]
            }
        
        return summary

    def get_anomaly_details(self, df: pd.DataFrame, results: Dict[str, Any], method: str = "knn") -> pd.DataFrame:
        """
        Get detailed information about detected anomalies.
        Returns a DataFrame with original data and anomaly scores.
        """
        if method not in results["anomaly_scores"]:
            raise ValueError(f"Method {method} not found in results")
        
        # Create a copy of the original data
        anomaly_df = df.copy()
        
        # Add anomaly scores
        for col in self.numeric_columns:
            anomaly_df[f"{col}_anomaly_score"] = results["anomaly_scores"][method][col]
        
        # Add anomaly flag
        anomaly_df["is_anomaly"] = anomaly_df.index.isin(results["detected_anomalies"][method]["indices"])
        
        return anomaly_df 