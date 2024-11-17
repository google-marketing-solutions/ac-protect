# Use an official Python runtime as a parent image
FROM python:3.11.5

# Accept PROJECT_ID as a build-time argument
ARG PROJECT_ID

# Set the PROJECT_ID as an environment variable
ENV PROJECT_ID=${PROJECT_ID}

# Set the working directory in the container
WORKDIR /ac-protect

# Copy the current directory contents into the container at /app
COPY . /ac-protect

# Install any needed packages specified in requirements.txt
RUN pip install --require-hashes --no-cache-dir --no-deps -r requirements.txt

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Run app.py when the container launches
CMD ["streamlit", "run", "app.py", "--server.port", "8080"]
