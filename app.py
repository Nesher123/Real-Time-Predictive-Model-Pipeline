import os
from uvicorn import run
from app.main import app

if __name__ == '__main__':
    # Run the app
    port = int(os.environ.get('PORT', 5000))
    run(app, host='localhost', port=port)
