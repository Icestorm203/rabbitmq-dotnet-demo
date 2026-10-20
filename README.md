# RabbitMQ .NET Demo

Учебно-практический backend-проект для изучения RabbitMQ и его интеграции с C#/.NET.

Проект демонстрирует асинхронную обработку задач, маршрутизацию сообщений через Direct Exchange, работу с несколькими очередями и consumers, ручные подтверждения сообщений и повторную доставку при сбое Worker.

## Архитектура

```text
                     ASP.NET Core API
                          Producer
                             |
                             | publish
                             v
                   jobs.direct.exchange
                     /              \
              routing key: ai    routing key: email
                   /                  \
                  v                    v
          jobs.ai.queue        jobs.email.queue
                  |                    |
                  v                    v
              Consumer             Consumer
                  \                    /
                   \                  /
                    \                /
                     .NET Worker Service
```

## Возможности

- ASP.NET Core REST API в роли Producer
- .NET Worker Service в роли Consumer
- RabbitMQ в Docker
- Direct Exchange
- Routing Keys
- Bindings между Exchange и очередями
- Несколько очередей для разных типов задач
- Durable Exchange и очереди
- Persistent messages
- Manual ACK/NACK
- Prefetch для ограничения количества неподтверждённых сообщений
- Несколько конкурирующих Consumers
- Распределение задач между несколькими Worker
- Повторная доставка сообщения при сбое Worker до ACK
- Проверка признака Redelivered
- RabbitMQ Management UI

## Стек

- C#
- .NET 10
- ASP.NET Core Web API
- .NET Worker Service
- RabbitMQ 4
- RabbitMQ.Client 7
- Docker
- Docker Compose
- REST API

## Структура проекта

```text
rabbitmq-dotnet-demo/
│
├── src/
│   ├── RabbitMqDemo.Api/
│   │   ├── Controllers/
│   │   │   └── JobsController.cs
│   │   ├── Messaging/
│   │   │   └── RabbitMqPublisher.cs
│   │   └── Program.cs
│   │
│   ├── RabbitMqDemo.Contracts/
│   │   └── JobMessage.cs
│   │
│   └── RabbitMqDemo.Worker/
│       ├── Program.cs
│       └── Worker.cs
│
├── docker-compose.yml
├── RabbitMqDemo.sln
└── README.md
```

## Как работает приложение

Клиент отправляет задачу через:

```http
POST /api/jobs
```

ASP.NET Core API формирует сообщение и публикует его в Direct Exchange:

```text
jobs.direct.exchange
```

Для маршрутизации используются два Routing Key:

```text
ai
email
```

Exchange связан с двумя очередями:

```text
routing key: ai
        |
        v
jobs.ai.queue
```

и:

```text
routing key: email
        |
        v
jobs.email.queue
```

Таким образом AI-задачи и задачи отправки электронной почты автоматически попадают в разные очереди.

## Запуск RabbitMQ

RabbitMQ запускается через Docker Compose:

```bash
docker compose up -d
```

Проверить состояние контейнера:

```bash
docker compose ps
```

RabbitMQ Management UI доступен по адресу:

```text
http://localhost:15672
```

Данные для локального окружения:

```text
Login: guest
Password: guest
```

> Эти данные используются только для локальной разработки.

## Запуск API

```bash
dotnet run --project src/RabbitMqDemo.Api
```

После запуска API будет принимать HTTP-запросы и публиковать сообщения в RabbitMQ.

## Запуск Worker

В отдельном терминале:

```bash
dotnet run --project src/RabbitMqDemo.Worker
```

Worker подключается к RabbitMQ и начинает слушать:

```text
jobs.ai.queue
jobs.email.queue
```

Можно одновременно запустить несколько экземпляров Worker, чтобы увидеть распределение задач между Consumers.

## Пример AI-задачи

Запрос:

```http
POST /api/jobs
Content-Type: application/json
```

```json
{
  "name": "generate-content",
  "payload": "Create marketing post with AI",
  "routingKey": "ai"
}
```

Маршрут сообщения:

```text
ASP.NET Core API
        |
        v
jobs.direct.exchange
        |
        | routing key: ai
        v
jobs.ai.queue
        |
        v
Worker
```

Пример ответа:

```json
{
  "routingKey": "ai",
  "status": "Queued"
}
```

## Пример Email-задачи

```json
{
  "name": "send-email",
  "payload": "Send weekly marketing report",
  "routingKey": "email"
}
```

Маршрут:

```text
ASP.NET Core API
        |
        v
jobs.direct.exchange
        |
        | routing key: email
        v
jobs.email.queue
        |
        v
Worker
```

## Direct Exchange и Routing

В проекте используется Direct Exchange:

```text
jobs.direct.exchange
```

Для него настроены Bindings:

```text
jobs.direct.exchange
        |
        +--- ai -----> jobs.ai.queue
        |
        +--- email --> jobs.email.queue
```

Direct Exchange позволяет направлять сообщение в очередь на основании Routing Key.

## Manual ACK/NACK

Consumers используют ручное подтверждение сообщений:

```text
autoAck = false
```

После успешной обработки Worker отправляет ACK:

```text
Message
   |
   v
Consumer
   |
   v
Processing
   |
   v
ACK
```

Если сообщение невозможно корректно обработать, Consumer может отправить NACK.

Это позволяет не считать задачу успешно выполненной до фактического завершения её обработки.

## Ready и Unacked

В RabbitMQ Management UI можно наблюдать состояния сообщений.

### Ready

Сообщение находится в очереди и ожидает Consumer:

```text
Queue
  |
  v
Ready
```

### Unacked

Сообщение уже передано Consumer, но Worker ещё не подтвердил успешную обработку:

```text
RabbitMQ
    |
    v
Consumer
    |
    v
Unacked
```

После ACK сообщение считается обработанным.

## Prefetch

Worker использует:

```text
prefetchCount = 1
```

Это ограничивает количество неподтверждённых сообщений, выдаваемых одному Consumer.

Например, при двух Worker:

```text
                  jobs.ai.queue
                      |
              +-------+-------+
              |               |
              v               v
          Worker #1       Worker #2
              |               |
           1 message       1 message
              |               |
             ACK             ACK
```

После подтверждения Worker может получить следующую задачу.

## Несколько Consumers

Проект позволяет запустить несколько экземпляров Worker:

```bash
dotnet run --project src/RabbitMqDemo.Worker
```

При нескольких Consumers RabbitMQ распределяет сообщения между ними.

Например:

```text
Worker #1:
job-1
job-3
job-5

Worker #2:
job-2
job-4
job-6
```

Фактический порядок распределения зависит от работы Consumers и RabbitMQ.

## Повторная доставка

В проекте также проверяется сценарий аварийного завершения Worker до отправки ACK.

```text
RabbitMQ
    |
    v
Worker #1 получает сообщение
    |
    v
Unacked
    |
    X Worker завершается до ACK
    |
    v
RabbitMQ возвращает сообщение
    |
    v
Worker #2
    |
    v
Redelivered = true
    |
    v
Processing
    |
    v
ACK
```

Таким образом неподтверждённое сообщение может быть доставлено другому Consumer повторно.

## Persistent Messages

Сообщения публикуются с настройкой:

```csharp
Persistent = true
```

Также сообщения содержат метаданные:

```text
Content-Type
MessageId
Timestamp
```

Exchange и очереди объявлены как durable.

## Идемпотентность

При использовании повторной доставки Consumer должен учитывать возможность получения одного сообщения более одного раза.

Для этого каждое сообщение имеет уникальный:

```text
JobMessage.Id
```

В реальном приложении этот идентификатор может использоваться для проверки того, была ли бизнес-операция уже выполнена.

Это особенно важно для операций, которые нельзя безопасно выполнять дважды, например:

- платежи;
- отправка уведомлений;
- публикация контента;
- изменение состояния данных.

## Что изучено в проекте

В процессе разработки были практически разобраны:

- Producer и Consumer
- Queue
- Connection
- Channel
- Exchange
- Direct Exchange
- Binding
- Routing Key
- Durable Queue
- Persistent Message
- Manual ACK/NACK
- Ready и Unacked
- Prefetch
- Competing Consumers
- Message Redelivery
- Идемпотентная обработка сообщений

## Цель проекта

Проект создан для практического изучения RabbitMQ в C#/.NET и понимания принципов построения асинхронных backend-систем на основе очередей сообщений.

Основное внимание уделено не только отправке и получению сообщений, но и маршрутизации, распределению задач между Workers и поведению системы при сбоях Consumers.