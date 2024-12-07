#!/bin/sh

cleanup() {
    echo "Terminating processes..."
    kill $PRODUCE_PID $TRANSFORM_PID
    kill $CONSUME_PID
    docker-compose down
    exit
}

trap cleanup SIGINT

python3 -m venv ./venv
source ./venv/bin/activate
pip install -r requirements.txt

docker-compose up -d

echo "Sleeping for 15 second so Docker Containers can run"
sleep 15

python produce_messages.py &
PRODUCE_PID=$!

python streaming_transforms.py &
TRANSFORM_PID=$!

python consume_messages.py &
CONSUME_PID=$!

wait $PRODUCE_PID $TRANSFORM_PID $CONSUME_PID
