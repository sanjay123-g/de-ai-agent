"""
api.py — FastAPI service exposing the query agent.
Run: uvicorn api:app --reload
"""
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from agents.query_agent import answer_question

app = FastAPI(title="de-ai-agent Query API")


class QuestionRequest(BaseModel):
    question: str


@app.post("/v1/nl-to-sql")
def nl_to_sql(req: QuestionRequest):
    result = answer_question(req.question)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return {
        "sql": result["sql"],
        "columns": result["columns"],
        "rows": result["rows"],
    }


@app.get("/health")
def health():
    return {"status": "ok"}
