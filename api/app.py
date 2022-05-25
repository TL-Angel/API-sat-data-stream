# -*- coding: utf-8 -*-
__author__ = "David Pastrana"
__copyright__ = "Copyright 2021, Robina"
__credits__ = ["David Pastrana"]
__license__ = "GPL"
__version__ = "2.0.0"
__email__ = "losphiereth@gmail.com"
__status__ = "Development"

import sys
import os
sys.path.append('../')  # noqa
import uvicorn
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi.middleware.cors import CORSMiddleware
from routes.datos import datos_routes
from config.cnn import MONGO_CLIENT


# :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
app = FastAPI()

def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="SAT Datos API",
        version="0.0.1",
        description="API de entrega de datos de la base del SAT de Robina",
        routes=app.routes,
    )
    # openapi_schema["info"]["x-logo"] = {"url": AUTH_CONFIG.app_logo}
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.include_router(datos_routes)
app.openapi = custom_openapi


# Add middleware to acept CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins='*',
    allow_credentials='',
    allow_methods='',
    allow_headers=''
    # expose_headers=AUTH_CONFIG.cors_expose_headers
)

# ::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
if __name__ == '__main__':

    uvicorn.run('app:app',
                host='0.0.0.0',
                port=8980,
                reload=True,
                debug=True,
                workers=1)
