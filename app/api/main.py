from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime
import os
from typing import Optional
import json

from app.services.storage import MinioClient
from app.services.normalizer import DataNormalizer
from app.services.analyzer import ConcentrationAnalyzer
from app.services.anomaly import AnomalyDetector
from app.llm.insights import InsightGenerator

app = FastAPI(title="Data Pipeline API")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
minio_client = MinioClient()
normalizer = DataNormalizer()
analyzer = ConcentrationAnalyzer()
anomaly_detector = AnomalyDetector()
insight_generator = InsightGenerator()

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload an Excel file for processing.
    The file will be normalized, analyzed, and stored in MinIO.
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Only Excel files are allowed")
    
    try:
        # Read the Excel file
        df = pd.read_excel(file.file)
        
        # Generate a unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{file.filename}"
        
        # Normalize the data
        normalized_df = normalizer.normalize(df)
        
        # Save to MinIO
        minio_client.save_dataframe(normalized_df, filename)
        
        return {
            "message": "File uploaded and processed successfully",
            "filename": filename
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/analyze/{filename}")
async def analyze_file(filename: str, top_n: Optional[int] = 10):
    """
    Analyze a previously uploaded file for concentration and anomalies.
    """
    try:
        # Load data from MinIO
        df = minio_client.load_dataframe(filename)
        
        # Perform concentration analysis
        concentration_results = analyzer.analyze_concentration(df, top_n)
        
        # Detect anomalies
        anomaly_results = anomaly_detector.detect_anomalies(df)
        
        # Generate insights
        insights = insight_generator.generate_insights(df, concentration_results, anomaly_results)
        
        return {
            "concentration": concentration_results,
            "anomalies": anomaly_results,
            "insights": insights
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {"status": "healthy"} 