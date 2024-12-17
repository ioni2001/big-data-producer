using Big.Data.DataProducer.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Big.Data.DataProducer.Workers;

public class SocialMediaCommentsWorker : BackgroundService
{
    private readonly IKafkaProducerService _kafkaProducerService;
    private readonly ILogger<SocialMediaCommentsWorker> _logger;
    private static List<(string, string)> mockedList = [("ioni", "ba e bine, imi place CG"), ("Anda", "nu e bine deloc, nu imi place Ciolacu"), ("Dulcu", "Mie imi place Lasconi")];

    public SocialMediaCommentsWorker(IKafkaProducerService kafkaProducerService, ILogger<SocialMediaCommentsWorker> logger)
    {
        _kafkaProducerService = kafkaProducerService;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Kafka Background Service simulator is starting.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var sendTasks = mockedList.Select(x => SendSingleEventAsync(x.Item1, x.Item2)).ToList();

                await Task.WhenAll(sendTasks);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while producing a Kafka message.");
            }

            // Wait for 10 seconds
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }

        _logger.LogInformation("Kafka Background Service is stopping.");
    }

    private async Task SendSingleEventAsync(string name, string comment)
    {
        try
        {
            await _kafkaProducerService.ProduceSocialMediaCommentsAsync(name, comment);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send Kafka event for social media comment {Comment} written by {WriterName}", comment, name);
        }
    }
}
