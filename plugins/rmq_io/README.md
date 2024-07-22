# Test

## Start RabbitMQ server locally

docker run -d --rm --name rabbitmq --hostname test -p 15672:15672 -p 5672:5672 rabbitmq:3-management
