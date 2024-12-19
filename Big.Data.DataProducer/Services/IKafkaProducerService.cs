using Big.Data.DataProducer.Models.Events;

namespace Big.Data.DataProducer.Services;

public interface IKafkaProducerService
{
    Task ProduceSocialMediaCommentsAsync(SocialMediaCommentEvent commentEvent);
}
