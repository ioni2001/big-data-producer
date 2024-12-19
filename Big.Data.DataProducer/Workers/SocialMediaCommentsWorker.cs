using Big.Data.DataProducer.Models.Events;
using Big.Data.DataProducer.Services;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Big.Data.DataProducer.Workers;

public class SocialMediaCommentsWorker : BackgroundService
{
    private readonly IKafkaProducerService _kafkaProducerService;
    private readonly ICsvParserService _csvParserService;
    private readonly ILogger<SocialMediaCommentsWorker> _logger;

    public SocialMediaCommentsWorker(
        IKafkaProducerService kafkaProducerService,
        ICsvParserService csvParserService,
        ILogger<SocialMediaCommentsWorker> logger)
    {
        _kafkaProducerService = kafkaProducerService;
        _csvParserService = csvParserService;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Kafka Background Service simulator is starting.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var sendTasks = GetRandomComments(15).Select(SendSingleEventAsync).ToList();

                await Task.WhenAll(sendTasks);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while producing a Kafka message.");
            }

            // Wait for 30 seconds
            await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
        }

        _logger.LogInformation("Kafka Background Service is stopping.");
    }

    private async Task SendSingleEventAsync(SocialMediaCommentEvent comment)
    {
        try
        {
            await _kafkaProducerService.ProduceSocialMediaCommentsAsync(comment);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send Kafka event for social media comment {Comment} written by {WriterName}", comment.Comment, comment.Name);
        }
    }

    private List<SocialMediaCommentEvent> GetRandomComments(int count)
    {
        var comments = _csvParserService.GetCommentData();
        var random = new Random();

        return comments.OrderBy(_ => random.Next())
            .Take(count)
            .ToList();
    }
}
