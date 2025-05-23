import pandas as pd
from typing import Dict, List, Any
from transformers import pipeline, AutoModelForCausalLM, AutoTokenizer
import json
import re

class InsightGenerator:
    def __init__(self):
        # Initialize with a more capable model
        self.model_name = "facebook/opt-350m"  # Using a larger model for better insights
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForCausalLM.from_pretrained(self.model_name)
        self.generator = pipeline(
            "text-generation",
            model=self.model,
            tokenizer=self.tokenizer,
            max_length=1000,  # Increased for more detailed insights
            num_return_sequences=1,
            temperature=0.8,  # Slightly increased for more creative responses
            top_p=0.95,  # Increased for better quality
            do_sample=True
        )

    def generate_insights(
        self,
        df: pd.DataFrame,
        concentration_results: Dict[str, Any],
        anomaly_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Generate insights from the data analysis results using the LLM.
        """
        # Prepare the context for the model
        context = self._prepare_context(df, concentration_results, anomaly_results)
        
        # Generate insights for different aspects
        insights = {
            "concentration_insights": self._generate_concentration_insights(context, df),
            "anomaly_insights": self._generate_anomaly_insights(context, df, anomaly_results),
            "recommendations": self._generate_recommendations(context, df, anomaly_results)
        }
        
        return insights

    def _prepare_context(
        self,
        df: pd.DataFrame,
        concentration_results: Dict[str, Any],
        anomaly_results: Dict[str, Any]
    ) -> str:
        """Prepare the context string for the LLM."""
        # Get basic statistics for numeric columns
        numeric_stats = df.select_dtypes(include=['number']).describe().to_dict()
        
        # Get value counts for categorical columns
        categorical_stats = {
            col: df[col].value_counts().head(5).to_dict()
            for col in df.select_dtypes(include=['object', 'category']).columns
        }
        
        # Get top values and their percentages
        top_values = {}
        for col in df.select_dtypes(include=['object', 'category']).columns:
            value_counts = df[col].value_counts(normalize=True).head(3)
            top_values[col] = {
                value: f"{percentage:.1%}" 
                for value, percentage in value_counts.items()
            }
        
        context = f"""
        Dataset Summary:
        - Total rows: {len(df)}
        - Total columns: {len(df.columns)}
        - Numeric columns: {len(df.select_dtypes(include=['number']).columns)}
        - Categorical columns: {len(df.select_dtypes(include=['object', 'category']).columns)}

        Column Names and Types:
        {json.dumps({col: str(dtype) for col, dtype in df.dtypes.items()}, indent=2)}

        Numeric Statistics:
        {json.dumps(numeric_stats, indent=2)}

        Top 3 Values in Categorical Columns (with percentages):
        {json.dumps(top_values, indent=2)}

        Concentration Analysis:
        {json.dumps(concentration_results['summary'], indent=2)}

        Anomaly Detection:
        {json.dumps(anomaly_results['summary'], indent=2)}
        """
        return context

    def _generate_concentration_insights(self, context: str, df: pd.DataFrame) -> List[str]:
        """Generate insights about data concentration."""
        # Get specific concentration metrics
        numeric_cols = df.select_dtypes(include=['number']).columns
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        
        concentration_metrics = []
        for col in numeric_cols:
            top_10_pct = df[col].quantile(0.9)
            bottom_10_pct = df[col].quantile(0.1)
            concentration_metrics.append(f"{col}: Top 10% > {top_10_pct:.2f}, Bottom 10% < {bottom_10_pct:.2f}")
        
        for col in categorical_cols:
            top_value = df[col].value_counts().iloc[0]
            top_value_pct = (top_value / len(df)) * 100
            concentration_metrics.append(f"{col}: Most common value appears {top_value} times ({top_value_pct:.1f}%)")
        
        metrics_str = "\n".join(concentration_metrics)
        
        prompt = f"""
        Analyze this data and provide exactly 3 insights about data concentration patterns.
        Each insight should be specific and include actual numbers from the data.

        Data:
        {context}

        Specific Concentration Metrics:
        {metrics_str}

        Write 3 numbered insights (1., 2., 3.) that describe the concentration patterns in the data.
        Each insight should include specific numbers and explain their business impact.
        """
        
        try:
            response = self.generator(prompt)[0]['generated_text']
            insights = self._parse_insights(response)
            if not insights:
                return self._generate_fallback_concentration_insights(df)
            return insights
        except Exception as e:
            print(f"Error generating concentration insights: {str(e)}")
            return self._generate_fallback_concentration_insights(df)

    def _generate_anomaly_insights(self, context: str, df: pd.DataFrame, anomaly_results: Dict[str, Any]) -> List[str]:
        """Generate insights about detected anomalies."""
        # Get specific anomaly metrics
        anomaly_metrics = []
        if 'detected_anomalies' in anomaly_results:
            for method, details in anomaly_results['detected_anomalies'].items():
                anomaly_metrics.append(
                    f"{method}: {details['count']} anomalies ({details['percentage']:.1f}%)"
                )
        
        metrics_str = "\n".join(anomaly_metrics)
        
        prompt = f"""
        Analyze this data and provide exactly 3 insights about detected anomalies.
        Each insight should be specific and include actual numbers from the data.

        Data:
        {context}

        Specific Anomaly Metrics:
        {metrics_str}

        Write 3 numbered insights (1., 2., 3.) that describe the anomalies found in the data.
        Each insight should include specific numbers and explain their business impact.
        """
        
        try:
            response = self.generator(prompt)[0]['generated_text']
            insights = self._parse_insights(response)
            if not insights:
                return self._generate_fallback_anomaly_insights(anomaly_results)
            return insights
        except Exception as e:
            print(f"Error generating anomaly insights: {str(e)}")
            return self._generate_fallback_anomaly_insights(anomaly_results)

    def _generate_recommendations(self, context: str, df: pd.DataFrame, anomaly_results: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on the analysis."""
        # Get specific metrics for recommendations
        metrics = []
        
        # Add concentration metrics
        for col in df.select_dtypes(include=['number']).columns:
            skew = df[col].skew()
            metrics.append(f"{col}: Skewness = {skew:.2f}")
        
        # Add anomaly metrics
        if 'detected_anomalies' in anomaly_results:
            for method, details in anomaly_results['detected_anomalies'].items():
                metrics.append(f"Anomalies ({method}): {details['count']} ({details['percentage']:.1f}%)")
        
        metrics_str = "\n".join(metrics)
        
        prompt = f"""
        Analyze this data and provide exactly 3 recommendations.
        Each recommendation should be specific and include actual numbers from the data.

        Data:
        {context}

        Specific Metrics:
        {metrics_str}

        Write 3 numbered recommendations (1., 2., 3.) that address the issues found in the data.
        Each recommendation should include specific actions and thresholds.
        """
        
        try:
            response = self.generator(prompt)[0]['generated_text']
            insights = self._parse_insights(response)
            if not insights:
                return self._generate_fallback_recommendations(df, anomaly_results)
            return insights
        except Exception as e:
            print(f"Error generating recommendations: {str(e)}")
            return self._generate_fallback_recommendations(df, anomaly_results)

    def _parse_insights(self, text: str) -> List[str]:
        """Parse the generated text into a list of insights."""
        # Remove any text before the first numbered item
        text = re.sub(r'^.*?(?=\d+\.)', '', text, flags=re.DOTALL)
        
        # Split by numbered points
        insights = []
        for line in text.split('\n'):
            line = line.strip()
            # Match lines that start with a number and period
            match = re.match(r'^\d+\.\s*(.+)$', line)
            if match:
                insight = match.group(1).strip()
                # Filter out instruction-like text
                if (insight and len(insight) > 10 and 
                    not any(keyword in insight.lower() for keyword in 
                           ['write', 'include', 'should', 'must', 'need', 'provide', 'analyze'])):
                    insights.append(insight)
        
        return insights[:3]  # Return top 3 insights

    def _generate_fallback_concentration_insights(self, df: pd.DataFrame) -> List[str]:
        """Generate fallback concentration insights based on actual data."""
        insights = []
        
        # Get top concentration metrics
        for col in df.select_dtypes(include=['number']).columns:
            top_10_pct = df[col].quantile(0.9)
            bottom_10_pct = df[col].quantile(0.1)
            insights.append(
                f"Column '{col}' shows high concentration: top 10% values are above {top_10_pct:.2f}, "
                f"while bottom 10% are below {bottom_10_pct:.2f}"
            )
        
        for col in df.select_dtypes(include=['object', 'category']).columns:
            top_value = df[col].value_counts().iloc[0]
            top_value_pct = (top_value / len(df)) * 100
            insights.append(
                f"Column '{col}' has high concentration: most common value appears {top_value} times "
                f"({top_value_pct:.1f}% of total)"
            )
        
        return insights[:3]

    def _generate_fallback_anomaly_insights(self, anomaly_results: Dict[str, Any]) -> List[str]:
        """Generate fallback anomaly insights based on actual results."""
        insights = []
        
        if 'detected_anomalies' in anomaly_results:
            for method, details in anomaly_results['detected_anomalies'].items():
                insights.append(
                    f"Anomaly detection method '{method}' identified {details['count']} anomalies "
                    f"({details['percentage']:.1f}% of total data)"
                )
        
        return insights[:3]

    def _generate_fallback_recommendations(self, df: pd.DataFrame, anomaly_results: Dict[str, Any]) -> List[str]:
        """Generate fallback recommendations based on actual data."""
        recommendations = []
        
        # Add recommendations based on data distribution
        for col in df.select_dtypes(include=['number']).columns:
            skew = df[col].skew()
            if abs(skew) > 1:
                recommendations.append(
                    f"Consider normalizing or transforming column '{col}' due to high skewness ({skew:.2f})"
                )
        
        # Add recommendations based on anomalies
        if 'detected_anomalies' in anomaly_results:
            for method, details in anomaly_results['detected_anomalies'].items():
                if details['percentage'] > 5:
                    recommendations.append(
                        f"Investigate the {details['count']} anomalies detected by {method} "
                        f"({details['percentage']:.1f}% of data)"
                    )
        
        return recommendations[:3] 