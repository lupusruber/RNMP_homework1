
# Рударење на масивни податоци: Домашна работа 1

This project runs a set of Python Kafka and Flink scripts in a Docker environment, simulating message production & consumption using Kafka, and transformation using Flink. All the dependencies are handled through a simple bash script that installs the required packages and runs the necessary Docker containers.

## Main technologies

- Kafka
- Flink
- Docker

## Requirements

To run the project, you only need to have the following installed on your Ubuntu machine:

- Docker
- Git
- Python 3

## Project Structure

- `produce_messages.py`: Python script that produces messages and sends them to Kafka.
- `streaming_transforms.py`: Transforms incoming messages in real-time.
- `consume_messages.py`: Consumes messages from Kafka and processes them.
- `docker-compose.yml`: Docker configuration for any required services.
- `requirements.txt`: Python dependencies for the project.

## Installation Steps

1. Clone the repository:

   ```bash
   git clone https://github.com/lupusruber/RNMP_homework1.git
   cd RNMP_homework1
   ```

2. Run the `start.sh` script:

   ```bash
   source start.sh
   ```

   This will:

   - Create a Python virtual environment.
   - Install the required Python dependencies from the `requirements.txt` file.
   - Start the necessary Docker containers.
   - Run the `produce_messages.py`, `streaming_transforms.py`, and `consume_messages.py` scripts.

3. The script will automatically handle the setup, so you don't need to worry about configuring the environment manually. Once the script is run, it will start processing messages in the background.

4. To stop the process, simply press `Ctrl + C`. The script will clean up all running processes and shut down the Docker containers.

## Cleanup

To stop the processes and bring down Docker containers, simply press `Ctrl + C` at any time. This will kill all running processes and clean up the containers.

## Docker Setup

Make sure Docker is installed and running on your system. The `start.sh` script will take care of running `docker-compose` to set up the containers.

## Python Virtual Environment

The `start.sh` script creates a virtual environment using Python 3, installs the required dependencies, and activates the environment automatically.

## Troubleshooting

If you encounter any issues, make sure you have Docker, Git, and Python installed on your machine. Also, ensure that your Docker daemon is running before starting the script.

For further assistance, please refer to the documentation or raise an issue in the repository.
