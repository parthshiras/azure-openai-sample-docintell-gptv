# Use a Python base image
FROM python:3.12

# Set working directory
WORKDIR /app

# Copy requirements.txt (if you have dependencies)
COPY requirements.txt .
RUN pip install -r requirements.txt  

# Copy your Python application and code files
COPY . .

# Expose the port used by Flask (usually 5000)
EXPOSE 5000

# Run the Flask app (replace 'app:app' with your app object name if different)
CMD ["python", "app.py"]
