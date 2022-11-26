# these will speed up builds, for docker-compose >= 1.25
export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_BUILDKIT=1

all: down build up test-allocation

build:
	docker-compose build

up:
	docker-compose up -d

up-postgres:
	docker-compose up -d postgres

down:
	docker-compose down --remove-orphans

test-allocation: up
	docker-compose run --rm --no-deps --entrypoint=pytest api /tests/test_allocation/unit /tests/test_allocation/integration /tests/test_allocation/e2e

unit-tests-allocation:
	docker-compose run --rm --no-deps --entrypoint=pytest api /tests/test_allocation/unit

integration-tests-allocation: up
	docker-compose run --rm --no-deps --entrypoint=pytest api /tests/test_allocation/integration

e2e-tests-allocation: up
	docker-compose run --rm --no-deps --entrypoint=pytest api /tests/test_allocation/e2e

logs:
	docker-compose logs --tail=25 api redis_pubsub
