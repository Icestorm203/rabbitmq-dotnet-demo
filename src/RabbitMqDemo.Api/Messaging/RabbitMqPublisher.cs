using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMqDemo.Contracts;

namespace RabbitMqDemo.Api.Messaging;

public class RabbitMqPublisher
{
    private const string ExchangeName = "jobs.direct.exchange";

    private const string AiQueueName = "jobs.ai.queue";
    private const string EmailQueueName = "jobs.email.queue";

    private const string AiRoutingKey = "ai";
    private const string EmailRoutingKey = "email";

    private readonly ConnectionFactory _factory;

    public RabbitMqPublisher()
    {
        _factory = new ConnectionFactory
        {
            HostName = "localhost",
            UserName = "guest",
            Password = "guest"
        };
    }

    public async Task PublishAsync(
        JobMessage message,
        string routingKey
    )
    {
        await using var connection =
            await _factory.CreateConnectionAsync();

        await using var channel =
            await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(
            exchange: ExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null
        );

        await channel.QueueDeclareAsync(
            queue: AiQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null
        );

        await channel.QueueDeclareAsync(
            queue: EmailQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null
        );

        await channel.QueueBindAsync(
            queue: AiQueueName,
            exchange: ExchangeName,
            routingKey: AiRoutingKey
        );

        await channel.QueueBindAsync(
            queue: EmailQueueName,
            exchange: ExchangeName,
            routingKey: EmailRoutingKey
        );

        var json = JsonSerializer.Serialize(message);
        var body = Encoding.UTF8.GetBytes(json);

        var properties = new BasicProperties
        {
            Persistent = true,
            ContentType = "application/json",
            MessageId = message.Id.ToString(),
            Timestamp = new AmqpTimestamp(
                DateTimeOffset.UtcNow.ToUnixTimeSeconds()
            )
        };
        
        await channel.BasicPublishAsync(
            exchange: ExchangeName,
            routingKey: routingKey,
            mandatory: true,
            basicProperties: properties,
            body: body
        );
    }
}