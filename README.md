
# Рударење на масивни податоци: Домашна работа 1

This project runs a set of Python Kafka and Flink scripts in a Docker environment, simulating message production & consumption using Kafka, and transformation using Flink. All the dependencies are handled through a simple bash script that installs the required packages (within a Python venv) and runs the necessary Docker containers.

## Main technologies

- Kafka
- Flink
- Docker

## Documentation

You can find the full documentation for the project in the PDF file below:

[Project Documentation](https://github.com/lupusruber/RNMP_homework1/blob/master/%D0%94%D0%BE%D0%BA%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%B0%D1%86%D0%B8%D1%98%D0%B0%20%D0%B7%D0%B0%20Flink%20%D0%B0%D0%BF%D0%BB%D0%B8%D0%BA%D0%B0%D1%86%D0%B8%D1%98%D0%B0%D1%82%D0%B0.pdf)

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

## Review Kafka and Flink Messages using the Kafka UI

You can review the messages produced by Kafka and Flink by navigating to the Kafka UI at the following address:

[http://localhost:8080](http://localhost:8080)

## Cleanup

To stop the processes and bring down Docker containers, simply press `Ctrl + C` at any time. This will kill all running processes and clean up the containers.

## Troubleshooting

- **Kafka Not Running:** If you are not using Docker, ensure that Kafka is running on the specified host (localhost:9092).
- **Dependencies:** Ensure all dependencies in requirements.txt are installed in the virtual environment.
- **Kafka Connection Issues:** Double-check your Kafka configurations in the scripts, especially BOOTSTRAP_SERVERS.

For further assistance, please refer to the documentation or raise an issue in the repository.
