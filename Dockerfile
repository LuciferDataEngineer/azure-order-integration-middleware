FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && curl -fsSLo /tmp/ms-repo.deb https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb \
 && echo '8434dcb8c346dc95fbd63dbece056c343704590b58b6a5c323d39acf52bf0b48  /tmp/ms-repo.deb' | sha256sum -c - \
 && dpkg -i /tmp/ms-repo.deb && apt-get update \
 && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
 && rm -rf /var/lib/apt/lists/* /tmp/ms-repo.deb
WORKDIR /app
COPY azure/export/requirements.txt /app/requirements.txt
RUN python -m pip install --no-cache-dir -r requirements.txt
COPY order_source.py order_export.py ./
USER 10001:10001
ENTRYPOINT ["python","order_export.py"]
CMD ["--export-cloud"]
