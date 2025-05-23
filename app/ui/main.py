import streamlit as st
import pandas as pd
import json
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go

from app.services.storage import MinioClient
from app.services.normalizer import DataNormalizer
from app.services.analyzer import ConcentrationAnalyzer
from app.services.anomaly import AnomalyDetector
from app.llm.insights import InsightGenerator

# Initialize services
minio_client = MinioClient()
normalizer = DataNormalizer()
analyzer = ConcentrationAnalyzer()
anomaly_detector = AnomalyDetector()
insight_generator = InsightGenerator()

def main():
    st.set_page_config(page_title="Data Pipeline", layout="wide")
    st.title("Data Analysis Pipeline")

    # Sidebar for file upload
    with st.sidebar:
        st.header("Data Upload")
        uploaded_file = st.file_uploader("Upload Excel File", type=['xlsx', 'xls'])
        
        if uploaded_file:
            st.success("File uploaded successfully!")
            
            # Process the file
            df = pd.read_excel(uploaded_file)
            normalized_df = normalizer.normalize(df)
            
            # Save to MinIO
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{timestamp}_{uploaded_file.name}"
            minio_client.save_dataframe(normalized_df, filename)
            
            st.session_state['current_file'] = filename
            st.session_state['data'] = normalized_df

    # Main content area
    if 'current_file' in st.session_state:
        df = st.session_state['data']
        
        # Data Preview
        st.header("Data Preview")
        st.dataframe(df.head())
        
        # Column Information
        st.header("Column Information")
        col_info = pd.DataFrame({
            'Type': df.dtypes,
            'Non-Null Count': df.count(),
            'Null Count': df.isnull().sum(),
            'Unique Values': df.nunique()
        })
        st.dataframe(col_info)
        
        # Analysis Section
        st.header("Analysis")
        
        # Select columns for analysis
        col1, col2 = st.columns(2)
        with col1:
            categorical_col = st.selectbox(
                "Select Categorical Column",
                options=df.select_dtypes(include=['object', 'category']).columns
            )
        with col2:
            numeric_col = st.selectbox(
                "Select Numeric Column",
                options=df.select_dtypes(include=['number']).columns
            )
        
        if st.button("Run Analysis"):
            # Perform concentration analysis
            concentration_results = analyzer.analyze_concentration(df, top_n=10)
            
            # Detect anomalies
            anomaly_results = anomaly_detector.detect_anomalies(df)
            
            # Generate insights
            insights = insight_generator.generate_insights(
                df,
                concentration_results,
                anomaly_results
            )
            
            # Display results
            st.subheader("Concentration Analysis")
            
            # Create concentration plot
            if categorical_col and numeric_col:
                # Calculate top percentages
                total = df[numeric_col].sum()
                top_10 = df.groupby(categorical_col)[numeric_col].sum().nlargest(10).sum() / total * 100
                top_20 = df.groupby(categorical_col)[numeric_col].sum().nlargest(20).sum() / total * 100
                top_50 = df.groupby(categorical_col)[numeric_col].sum().nlargest(50).sum() / total * 100
                
                # Create bar chart
                fig = go.Figure(data=[
                    go.Bar(
                        x=['Top 10%', 'Top 20%', 'Top 50%'],
                        y=[top_10, top_20, top_50],
                        text=[f'{top_10:.1f}%', f'{top_20:.1f}%', f'{top_50:.1f}%'],
                        textposition='auto',
                    )
                ])
                fig.update_layout(
                    title='Concentration Analysis',
                    yaxis_title='Percentage of Total',
                    showlegend=False
                )
                st.plotly_chart(fig)
            
            # Display insights
            st.subheader("AI-Generated Insights")
            
            # Concentration Insights
            st.markdown("### Concentration Insights")
            if insights.get("concentration_insights"):
                for i, insight in enumerate(insights["concentration_insights"], 1):
                    st.markdown(f"{i}. {insight}")
            else:
                st.info("No concentration insights available.")
            
            # Anomaly Insights
            st.markdown("### Anomaly Insights")
            if insights.get("anomaly_insights"):
                for i, insight in enumerate(insights["anomaly_insights"], 1):
                    st.markdown(f"{i}. {insight}")
            else:
                st.info("No anomaly insights available.")
            
            # Recommendations
            st.markdown("### Recommendations")
            if insights.get("recommendations"):
                for i, recommendation in enumerate(insights["recommendations"], 1):
                    st.markdown(f"{i}. {recommendation}")
            else:
                st.info("No recommendations available.")
            
            # Anomaly Detection
            st.subheader("Anomaly Detection")
            anomaly_df = anomaly_detector.get_anomaly_details(df, anomaly_results)
            
            # Display anomaly summary
            total_anomalies = anomaly_df['is_anomaly'].sum()
            anomaly_percentage = (total_anomalies / len(anomaly_df)) * 100
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Anomalies", total_anomalies)
            with col2:
                st.metric("Anomaly Percentage", f"{anomaly_percentage:.2f}%")
            
            # Create tabs for different views
            tab1, tab2 = st.tabs(["Anomaly Details", "Anomaly Distribution"])
            
            with tab1:
                # Display detailed anomaly information
                st.markdown("### Detailed Anomaly Information")
                if total_anomalies > 0:
                    # Get columns with anomaly scores
                    anomaly_score_cols = [col for col in anomaly_df.columns if col.endswith('_anomaly_score')]
                    
                    # Create a more informative display
                    display_df = anomaly_df[anomaly_df['is_anomaly']].copy()
                    
                    # Add original columns and anomaly scores
                    display_cols = [col for col in df.columns] + anomaly_score_cols
                    display_df = display_df[display_cols]
                    
                    # Sort by highest anomaly score
                    if anomaly_score_cols:
                        display_df = display_df.sort_values(by=anomaly_score_cols[0], ascending=False)
                    
                    st.dataframe(display_df)
                    
                    # Download option for anomaly data
                    if st.button("Download Anomaly Data"):
                        output_path = Path("data/output") / f"{st.session_state['current_file']}_anomalies.xlsx"
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        display_df.to_excel(output_path, index=False)
                        st.success(f"Anomaly data saved to {output_path}")
                else:
                    st.info("No anomalies detected in the dataset.")
            
            with tab2:
                # Display anomaly distribution
                st.markdown("### Anomaly Distribution")
                if total_anomalies > 0:
                    # Get the first anomaly score column
                    if anomaly_score_cols:
                        score_col = anomaly_score_cols[0]
                        
                        # Create histogram of anomaly scores
                        fig = px.histogram(
                            anomaly_df,
                            x=score_col,
                            color='is_anomaly',
                            title='Distribution of Anomaly Scores',
                            labels={score_col: 'Anomaly Score', 'is_anomaly': 'Is Anomaly'},
                            color_discrete_sequence=['blue', 'red']
                        )
                        st.plotly_chart(fig)
                        
                        # Create scatter plot for top anomalies
                        if len(display_df) > 0:
                            # Select two numeric columns for the scatter plot
                            numeric_cols = df.select_dtypes(include=['number']).columns
                            if len(numeric_cols) >= 2:
                                x_col = numeric_cols[0]
                                y_col = numeric_cols[1]
                                
                                fig = px.scatter(
                                    anomaly_df,
                                    x=x_col,
                                    y=y_col,
                                    color='is_anomaly',
                                    title=f'Anomalies in {x_col} vs {y_col}',
                                    labels={'is_anomaly': 'Is Anomaly'},
                                    color_discrete_sequence=['blue', 'red']
                                )
                                st.plotly_chart(fig)
                else:
                    st.info("No anomalies to display distribution.")

            # Download options
            st.subheader("Download Results")
            if st.button("Download Full Analysis Results"):
                # Save results to Excel
                output_path = Path("data/output") / f"{st.session_state['current_file']}_analysis.xlsx"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                anomaly_df.to_excel(output_path, index=False)
                st.success(f"Results saved to {output_path}")

if __name__ == "__main__":
    main() 