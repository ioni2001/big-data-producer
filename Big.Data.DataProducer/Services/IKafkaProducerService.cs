namespace Big.Data.DataProducer.Services;

public interface IKafkaProducerService
{
    Task ProduceSocialMediaCommentsAsync(string name, string comment);
}
