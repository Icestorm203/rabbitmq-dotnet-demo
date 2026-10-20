using Microsoft.AspNetCore.Mvc;
using RabbitMqDemo.Api.Messaging;
using RabbitMqDemo.Contracts;

namespace RabbitMqDemo.Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class JobsController : ControllerBase
{
    private static readonly HashSet<string> AllowedRoutingKeys =
    [
        "ai",
        "email"
    ];

    private readonly RabbitMqPublisher _publisher;

    public JobsController(RabbitMqPublisher publisher)
    {
        _publisher = publisher;
    }

    [HttpPost]
    public async Task<IActionResult> CreateJob(
        CreateJobRequest request
    )
    {
        var routingKey = request.RoutingKey
            .Trim()
            .ToLowerInvariant();

        if (!AllowedRoutingKeys.Contains(routingKey))
        {
            return BadRequest(new
            {
                Error = "RoutingKey must be 'ai' or 'email'."
            });
        }

        var message = new JobMessage(
            Guid.NewGuid(),
            request.Name,
            request.Payload,
            DateTime.UtcNow
        );

        await _publisher.PublishAsync(
            message,
            routingKey
        );

        return Accepted(new
        {
            message.Id,
            RoutingKey = routingKey,
            Status = "Queued"
        });
    }
}

public record CreateJobRequest(
    string Name,
    string Payload,
    string RoutingKey
);