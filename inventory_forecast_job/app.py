from fastapi import FastAPI, HTTPException, Header
from pydantic import BaseModel, Field
from datetime import datetime, date
import os

from forecast_production import run_forecast

app = FastAPI(title="Amazon Inventory Forecast Webhook", version="1.0")


class RunRequest(BaseModel):
    target_date: str = Field(..., description="YYYY-MM-DD")
    marketplaces: list[str] | None = Field(
        default=None,
        description="Optional list like ['USA','CA','UK','DE']. If omitted -> all default."
    )
    verbose: bool = Field(default=False, description="Print verbose logs")


def parse_iso_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        raise HTTPException(status_code=400, detail="target_date must be in YYYY-MM-DD format")


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/run")
def run(
    payload: RunRequest,
    authorization: str | None = Header(default=None),
):
    """
    Optional auth:
    - Set WEBHOOK_TOKEN in Railway Variables
    - n8n sends header: Authorization: Bearer <WEBHOOK_TOKEN>
    """
    required = os.environ.get("WEBHOOK_TOKEN")
    if required:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="Missing Authorization Bearer token")
        token = authorization.replace("Bearer ", "").strip()
        if token != required:
            raise HTTPException(status_code=403, detail="Invalid token")

    target = parse_iso_date(payload.target_date)

    result = run_forecast(
        target_date=target,
        marketplaces=payload.marketplaces,
        verbose=payload.verbose
    )
    return result
