from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from app.services import root, predict

router = APIRouter()
router.get('/', response_class=HTMLResponse)(root)
router.get('/predict')(predict)
