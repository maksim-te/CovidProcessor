# Приложение для обработки данных о COVID-19

## Сборка и push

1. Собрать jar file
```bash 
sbt assembly
```

Выполнить сборку образа и push в реестр
```bash
docker buildx build --platform linux/amd64 -t cr.yandex/crpeqnl53oh327o27qtu/spark/covid-processor:latest .
docker push cr.yandex/crpeqnl53oh327o27qtu/spark/covid-processor:latest
```
