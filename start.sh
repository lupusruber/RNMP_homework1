#!/bin/sh
python -m venv ./venv
pip install -r requirements.txt
docker-compose up -d
python produce_messages.py
python streaming_transforms.py
python consume_messages.py
