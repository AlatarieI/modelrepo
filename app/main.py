from fastapi import FastAPI, Cookie, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import database_manager as db
import uuid

app = FastAPI()

# Allow cross-origin requests (needed for local development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database when app starts
@app.on_event("startup")
def startup():
    db.init_database()
    print("Database initialized!")

# Home page - just a simple welcome
@app.get("/")
def home(user_id: str = Cookie(None), response: Response = None):
    if not user_id:
        user_id = str(uuid.uuid4())
        response.set_cookie(key="user_id", value=user_id, max_age=31536000)
    return {"message": "3D Model Viewer API", "your_session_id": user_id}

# Get all models as JSON (for API use)
@app.get("/api/models")
def get_models():
    """Returns all models as JSON data"""
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT model_id, model_name, format, model_description, 
                       polygon_count, preview_file, average_rating, download_date
                FROM Model
                ORDER BY download_date DESC
            """)
            
            # Convert rows to dictionaries
            columns = [desc[0] for desc in cur.description]
            models = []
            for row in cur.fetchall():
                models.append(dict(zip(columns, row)))
            
            return {"models": models, "count": len(models)}

# Show models in a simple HTML page
@app.get("/models", response_class=HTMLResponse)
def show_models_page():
    """Returns an HTML page displaying all models"""
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT model_id, model_name, format, model_description, 
                       polygon_count, preview_file, average_rating, download_date
                FROM Model
                ORDER BY download_date DESC
            """)
            
            models = cur.fetchall()
    
    # Build simple HTML
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>3D Models</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f5f5f5;
            }
            h1 {
                color: #333;
            }
            .model-grid {
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
                gap: 20px;
                margin-top: 20px;
            }
            .model-card {
                background: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            .model-card h3 {
                margin-top: 0;
                color: #2c3e50;
            }
            .model-info {
                color: #666;
                font-size: 14px;
                margin: 5px 0;
            }
            .rating {
                color: #f39c12;
                font-weight: bold;
            }
            .no-models {
                text-align: center;
                padding: 40px;
                color: #999;
            }
        </style>
    </head>
    <body>
        <h1>3D Model Library</h1>
        <p>Total models: """ + str(len(models)) + """</p>
    """
    
    if models:
        html += '<div class="model-grid">'
        for model in models:
            model_id, name, fmt, desc, poly_count, preview, rating, date = model
            html += f"""
            <div class="model-card">
                <h3>{name}</h3>
                <div class="model-info">Format: {fmt or 'Unknown'}</div>
                <div class="model-info">Polygons: {poly_count or 'N/A'}</div>
                <div class="model-info">Rating: <span class="rating">{'⭐' * int(rating or 0)}</span> ({rating or 0}/5)</div>
                <div class="model-info">Uploaded: {date or 'Unknown'}</div>
                {f'<p>{desc}</p>' if desc else ''}
            </div>
            """
        html += '</div>'
    else:
        html += '<div class="no-models">No models uploaded yet.</div>'
    
    html += """
    </body>
    </html>
    """
    
    return html

# Test database connection
@app.get("/test-db")
def test_db():
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()
                return {"status": "success", "db_version": version[0]}
    except Exception as e:
        return {"status": "error", "message": str(e)}