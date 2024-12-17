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

    public async Task ProduceSocialMediaCommentsAsync(string name, string comment)
    {
        var key = JsonConvert.SerializeObject(new JsonKey
        {
            Name = name,
        });

        var value = new SocialMediaCommentEvent
        {
            Name = name,
            Date = DateTime.Now.ToString(),
            Comment = comment
        };

        var result = await _producers["social-media-comments-producer"].ProduceAsync(key, value);

        _logger.LogInformation("Social Media Comment {Comment} written by {WriterName} was successfully sent to Kafka" +
            " {KafkaContext.Topic} {KafkaContext.Partition} {KafkaContext.Offset}", comment, name, result.Topic, result.Partition, result.Offset);
    }
}
