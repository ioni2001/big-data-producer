using Big.Data.DataProducer.Models.Events;
using KafkaFlow.Producers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Big.Data.DataProducer.Services;

public class KafkaProducerService : IKafkaProducerService
{
    private readonly IProducerAccessor _producers;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(IProducerAccessor producers, ILogger<KafkaProducerService> logger)
    {
        _producers = producers;
        _logger = logger;
    }

    public async Task ProduceSocialMediaCommentsAsync(SocialMediaCommentEvent commentEvent)
    {
        var key = JsonConvert.SerializeObject(new JsonKey
        {
            Name = commentEvent.Name,
        });

        var result = await _producers["social-media-comments-producer"].ProduceAsync(key, commentEvent);

        _logger.LogInformation("Social Media Comment {Comment} written by {WriterName} was successfully sent to Kafka" +
            " {KafkaContext.Topic} {KafkaContext.Partition} {KafkaContext.Offset}", commentEvent.Comment, commentEvent.Name, result.Topic, result.Partition, result.Offset);
    }
}
