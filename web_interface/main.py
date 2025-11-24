from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import sys
import os

# Add the parent directory to Python path to import the BGP analysis code
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from RAG.rag_framework.simple_router import SimpleBGPRouter
from RAG.rag_framework.clean_router import CleanBGPRouter
# from RAG.chatbgp_clean.chatbgp.router import ChatBGPRouter



app = FastAPI(title="BGP Analysis Chat Interface")
templates = Jinja2Templates(directory="web_interface/templates")
app.mount("/static", StaticFiles(directory="web_interface/static"), name="static")

# Initialize the BGP router
# router = SimpleBGPRouter()
# router = ChatBGPRouter()
router = CleanBGPRouter()

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Render the chat interface."""
    return templates.TemplateResponse(
        "chat.html",
        {
            "request": request,
            "example_queries": [
                "Check for route flaps in prefix 84.205.66.0/24",
                "What is the current state of prefix 201.71.181.0/24?",
                "Are there any BGP anomalies for AS8283?",
                "Show me the AS path for 84.205.66.0/24"
            ]
        }
    )

@app.post("/chat")
async def chat(request: Request, message: str = Form(...)):
    """Handle chat messages and return BGP analysis."""
    try:
        # Query the BGP router
        result = router.query(message)
        
        # Extract relevant information
        answer = result.get("answer", "Sorry, I couldn't analyze that query.")
        
        # Get BGP data if available
        bgp_data = result.get("bgp_data", {})
        live_state = bgp_data.get("live_bgp_state", {})
        current_state = live_state.get("current_state", {})
        
        # Check for route flaps
        flap_analysis = None
        if current_state and "validation" in current_state:
            analysis = current_state["validation"].get("analysis", {})
            flap_analysis = analysis.get("flap_analysis")
        
        return {
            "answer": answer,
            "flap_analysis": flap_analysis,
            "current_state": current_state
        }
        
    except Exception as e:
        return {
            "answer": f"Error processing query: {str(e)}",
            "flap_analysis": None,
            "current_state": None
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 