# 1. Base Image: Python 3.11 on Debian 12 (Bookworm) - STABLE
# We specifically use "bookworm" because it guarantees Java 17 availability.
FROM python:3.11-slim-bookworm

# 2. Install OpenJDK 17 and 'procps' (Required for Hadoop)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps && \
    apt-get clean;

# 3. Set JAVA_HOME (Standard path for Debian 12)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 4. CRITICAL: Compatibility Flags for Java 17 + Spark
# This unlocks Java internal components to prevent "InaccessibleObjectException"
ENV JDK_JAVA_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED \
                      --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
                      --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
                      --add-opens=java.base/java.io=ALL-UNNAMED \
                      --add-opens=java.base/java.net=ALL-UNNAMED \
                      --add-opens=java.base/java.nio=ALL-UNNAMED \
                      --add-opens=java.base/java.util=ALL-UNNAMED \
                      --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
                      --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
                      --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
                      --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
                      --add-opens=java.base/sun.security.action=ALL-UNNAMED \
                      --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
                      --add-opens=java.security.jgss/sun.security.jgss.krb5=ALL-UNNAMED"

# 5. Set PySpark Variables
ENV PYSPARK_PYTHON=python3

# 6. Setup Workspace
WORKDIR /app
COPY . /app

# 7. Install PySpark
RUN pip install --upgrade pip && \
    pip install pyspark

# 8. Run command
CMD ["python", "main.py"]