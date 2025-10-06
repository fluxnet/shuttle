FROM python:3.13-slim

WORKDIR /app

# Copy package files
COPY pyproject.toml ./
COPY README.md ./
COPY LICENSE ./

# Copy source code
COPY src/ ./src/

# Install the library
RUN pip install --upgrade pip && pip install .

# Set up environment
ENV PYTHONPATH=/app/src

# Default command to demonstrate library import
CMD ["python", "-c", "from fluxnet_shuttle_lib import main; main(); print('FLUXNET Shuttle Library main function executed successfully!')"]