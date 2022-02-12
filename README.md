﻿# Fast API Assignment address

## Setup Instructions

### Downlaod/Clone Repository

```bash
git clone git@github.com:Pradeep-raj544/fastapi-address-assaignment.git
```

### Install Required Python Modules

```bash
cd fastapi-address-assaignment
pip install -r requirements.txt
```
note: activate a virtual environment before installing requirements
### Start Web Server

To start the web server you need to run the following sequence of commands.

Next run the api web server.
```bash
uvicorn address-app.main:app --reload
```
Open Swagger UI from your browser to test out the API

```buildoutcfg
http://127.0.0.1:8000/docs
```
