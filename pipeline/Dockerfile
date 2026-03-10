# base Docker image that we will build on
# FROM python:3.13.11-slim

# set up our image by installing prerequisites; pandas in this case
# RUN pip install pandas pyarrow
# set up the working directory inside the container
# WORKDIR /code
# # copy the script to the container. 1st name is source file, 2nd is destination
# COPY pipeline.py .

# # define what to do first when the container runs
# # excute it when we do. docker run
# ENTRYPOINT ["python", "pipeline.py"]

#Docker with uv
# Start with slim Python 3.13 image
FROM python:3.13.10-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/


# Set working directory
WORKDIR /code

#copy these files we got from install uv library
# Copy dependency files first (better layer caching)
COPY pyproject.toml uv.lock .python-version ./


# Add virtual environment to PATH so we can use installed packages
# from ENTRYPOINT ["uv", "run", "python", "pipeline.py"] to ENTRYPOINT ["python", "pipeline.py"]
ENV PATH="/code/.venv/bin:$PATH"


# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked

# Copy application code
COPY ingest_data.py .

# Set entry point
ENTRYPOINT ["python", "ingest_data.py"]
# docker run -it --rm  test:pandas 12