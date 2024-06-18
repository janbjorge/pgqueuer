FROM postgres:16
ENV POSTGRES_USER=testuser
ENV POSTGRES_PASSWORD=testpassword

# Install Python and Pip
ENV VIRTUAL_ENV=/opt/venv
RUN apt-get update && \
    apt-get install -y python3-pip python3-full git && \
    rm -rf /var/lib/apt/lists/*  && \
    python3 -m venv $VIRTUAL_ENV

RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY . /src/
RUN cd src && pip install -e ".[dev]"

# # Copy the combined database setup script into the container
COPY --chmod=0755 test/db/init_db.sh /docker-entrypoint-initdb.d/init_db.sh
