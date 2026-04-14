FROM quay.io/astronomer/astro-runtime:12.1.1

# Install OS-level packages (if needed)
# Uncomment and add packages to packages.txt if required
# COPY packages.txt .
# RUN apt-get update && cat packages.txt | xargs apt-get install -y

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
