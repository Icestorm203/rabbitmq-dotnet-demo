using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMqDemo.Contracts;

namespace RabbitMqDemo.Worker;

public class Worker : BackgroundService
{
    private const string ExchangeName = "jobs.direct.exchange";

    private const string AiQueueName = "jobs.ai.queue";
    private const string EmailQueueName = "jobs.email.queue";

    private const string AiRoutingKey = "ai";
    private const string EmailRoutingKey = "email";

    private IConnection? _connection;
    private IChannel? _channel;

    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken
    )
    {
        var factory = new ConnectionFactory
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };

        _connection = await factory.CreateConnectionAsync(
            stoppingToken
        );

        _channel = await _connection.CreateChannelAsync(
            cancellationToken: stoppingToken
        );

        await _channel.BasicQosAsync(
            prefetchSize: 0,
            prefetchCount: 1,
            global: false,
            cancellationToken: stoppingToken
        );

        await DeclareTopologyAsync(stoppingToken);

        await StartConsumerAsync(
            AiQueueName,
            stoppingToken
        );

        await StartConsumerAsync(
            EmailQueueName,
            stoppingToken
        );

        _logger.LogInformation(
            "Consumers started for {AiQueue} and {EmailQueue}",
            AiQueueName,
            EmailQueueName
        );

        await Task.Delay(
            Timeout.Infinite,
            stoppingToken
        );
    }

    private async Task DeclareTopologyAsync(
        CancellationToken cancellationToken
    )
    {
        await _channel!.ExchangeDeclareAsync(
            exchange: ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken
        );

        await _channel.QueueDeclareAsync(
            queue: AiQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken
        );

        await _channel.QueueDeclareAsync(
            queue: EmailQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null,
            cancellationToken: cancellationToken
        );

        await _channel.QueueBindAsync(
            queue: AiQueueName,
            exchange: ExchangeName,
            routingKey: AiRoutingKey,
            cancellationToken: cancellationToken
        );

        await _channel.QueueBindAsync(
            queue: EmailQueueName,
            exchange: ExchangeName,
            routingKey: EmailRoutingKey,
            cancellationToken: cancellationToken
        );
    }

    private async Task StartConsumerAsync(
        string queueName,
        CancellationToken cancellationToken
    )
    {
        var consumer =
            new AsyncEventingBasicConsumer(_channel!);

        consumer.ReceivedAsync += async (_, eventArgs) =>
        {
            var body = eventArgs.Body.ToArray();
            var json = Encoding.UTF8.GetString(body);

            var message =
                JsonSerializer.Deserialize<JobMessage>(json);

            if (message is null)
            {
                _logger.LogWarning(
                    "Invalid message received from {Queue}",
                    queueName
                );

                await _channel!.BasicNackAsync(
                    deliveryTag: eventArgs.DeliveryTag,
                    multiple: false,
                    requeue: false
                );

                return;
            }

            _logger.LogInformation(
                "Received from {Queue} | RoutingKey: {RoutingKey} | " +
                "Job: {Id} | {Name} | {Payload} | " +
                "Redelivered: {Redelivered}",
                queueName,
                eventArgs.RoutingKey,
                message.Id,
                message.Name,
                message.Payload,
                eventArgs.Redelivered
            );

            await Task.Delay(3000);

            _logger.LogInformation(
                "Job {Id} processed successfully",
                message.Id
            );

            await _channel!.BasicAckAsync(
                deliveryTag: eventArgs.DeliveryTag,
                multiple: false
            );
        };

        await _channel!.BasicConsumeAsync(
            queue: queueName,
            autoAck: false,
            consumer: consumer,
            cancellationToken: cancellationToken
        );
    }

    public override async Task StopAsync(
        CancellationToken cancellationToken
    )
    {
        if (_channel is not null)
        {
            await _channel.CloseAsync(cancellationToken);
            await _channel.DisposeAsync();
        }

        if (_connection is not null)
        {
            await _connection.CloseAsync(
                cancellationToken
            );

            await _connection.DisposeAsync();
        }

        await base.StopAsync(cancellationToken);
    }
}