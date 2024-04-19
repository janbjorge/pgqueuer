FROM postgres:16
ENV POSTGRES_USER=testuser
ENV POSTGRES_PASSWORD=testpassword

# Copy the combined database setup script into the container
COPY ./init_db.sh /docker-entrypoint-initdb.d/init_db.sh

# Ensure the script is executable
RUN chmod +x /docker-entrypoint-initdb.d/init_db.sh
