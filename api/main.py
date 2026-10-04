from fastapi import FastAPI
from pydantic import BaseModel
from kafka import KafkaProducer
import json
from datetime import datetime


app = FastAPI(
    title="Real-Time Loan Fraud Detection API",
    description="Loan application API connected to Kafka",
    version="1.0"
)


# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# Loan application model
class LoanApplication(BaseModel):
    customer_id: str
    income: float
    loan_amount: float
    credit_score: int
    employment_years: int
    existing_loans: int
    monthly_debt: float
    device_id: str
    ip_address: str
    location: str


@app.get("/")
def home():
    return {
        "message": "Loan Fraud Detection API is running"
    }


@app.post("/loan-applications")
def create_loan_application(application: LoanApplication):

    loan_data = application.model_dump()

    loan_data["application_id"] = (
        f"LN{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    )

    loan_data["timestamp"] = datetime.now().isoformat()

    producer.send(
        "loan_applications",
        value=loan_data
    )

    producer.flush()

    return {
        "message": "Loan application sent to Kafka",
        "data": loan_data
    }