FROM python:3.13-slim

# The /app directory should act as the main application directory
WORKDIR /app

# Copy wordle words
COPY Data/GRI_2017_2020.xlsx /app

COPY Data /app/Data

# Copy the app package
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Kopier koden
COPY . .

# Start the app using serve command
CMD ["python","src/pdf_reader.py"]