"""Main application entry point."""

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse

from app.api.calculations import router as calculations_router
from app.core.config import settings

app = FastAPI(
    title="Financial Calculations API",
    description="Dynamic calculation builder for financial data",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(calculations_router, prefix="/api")

# Serve static files (UI)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

@app.get("/")
async def root():
    """Root endpoint with navigation to both interfaces."""
    return {
        "message": "Financial Calculations & Report Builder API",
        "interfaces": {
            "calculation_builder": "/calculation-builder",
            "report_builder": "/report-builder",
            "api_docs": "/docs"
        }
    }

@app.get("/calculation-builder")
async def calculation_builder():
    """Redirect to calculation builder interface."""
    return RedirectResponse(url="/static/calculation_ui.html")

@app.get("/report-builder")
async def report_builder():
    """Redirect to report builder interface."""
    return RedirectResponse(url="/static/report_builder.html")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )