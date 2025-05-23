#!/usr/bin/env python3
import argparse
import pandas as pd
from pathlib import Path
import json
from datetime import datetime

from app.services.storage import MinioClient
from app.services.normalizer import DataNormalizer
from app.services.analyzer import ConcentrationAnalyzer
from app.services.anomaly import AnomalyDetector
from app.llm.insights import InsightGenerator

def parse_args():
    parser = argparse.ArgumentParser(description='Analyze data files')
    parser.add_argument('--input', '-i', required=True, help='Input Excel file path')
    parser.add_argument('--output', '-o', help='Output directory for results')
    parser.add_argument('--top-n', type=int, default=10, help='Number of top items to analyze')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Initialize services
    minio_client = MinioClient()
    normalizer = DataNormalizer()
    analyzer = ConcentrationAnalyzer()
    anomaly_detector = AnomalyDetector()
    insight_generator = InsightGenerator()
    
    # Read input file
    print(f"Reading input file: {args.input}")
    df = pd.read_excel(args.input)
    
    # Normalize data
    print("Normalizing data...")
    normalized_df = normalizer.normalize(df)
    
    # Generate filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{Path(args.input).stem}"
    
    # Save to MinIO
    print("Saving to MinIO...")
    minio_client.save_dataframe(normalized_df, filename)
    
    # Perform analysis
    print("Analyzing concentration...")
    concentration_results = analyzer.analyze_concentration(normalized_df, args.top_n)
    
    print("Detecting anomalies...")
    anomaly_results = anomaly_detector.detect_anomalies(normalized_df)
    
    print("Generating insights...")
    insights = insight_generator.generate_insights(
        normalized_df,
        concentration_results,
        anomaly_results
    )
    
    # Prepare results
    results = {
        "filename": filename,
        "concentration_analysis": concentration_results,
        "anomaly_detection": anomaly_results,
        "insights": insights
    }
    
    # Save results
    if args.output:
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save JSON results
        json_path = output_dir / f"{filename}_analysis.json"
        with open(json_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Save Excel with anomaly scores
        excel_path = output_dir / f"{filename}_analysis.xlsx"
        anomaly_df = anomaly_detector.get_anomaly_details(
            normalized_df,
            anomaly_results
        )
        anomaly_df.to_excel(excel_path, index=False)
        
        print(f"\nResults saved to:")
        print(f"- JSON: {json_path}")
        print(f"- Excel: {excel_path}")
    else:
        # Print results to console
        print("\nAnalysis Results:")
        print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main() 