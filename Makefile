run:
	cd astro_runtime ; SA_GCP_PATH=$(SA_GCP_PATH) docker-compose up -d

stop:
	cd astro_runtime ; docker-compose down

restart:
	cd astro_runtime ; SA_GCP_PATH=$(SA_GCP_PATH) docker-compose restart