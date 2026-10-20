namespace RabbitMqDemo.Contracts;

public record JobMessage(
    Guid Id,
    string Name,
    string Payload,
    DateTime CreatedAt
);