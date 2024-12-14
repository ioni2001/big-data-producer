using Big.Data.DataProducer.Models.Configuration;
using Big.Data.DataProducer.Models.Events;
using Big.Data.DataProducer.Services;
using Big.Data.DataProducer.Worker;
using KafkaFlow;
using KafkaFlow.Configuration;
using KafkaFlow.Serializer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Logs;

namespace Big.Data.DataProducer;

public class Program
{
    private static ILogger<Program>? _logger;

    static async Task Main(string[] args)
    {
        IHost appHost = Host
            .CreateDefaultBuilder(args)
            .UseDefaultServiceProvider((context, options) =>
            {
                options.ValidateScopes = true;
            })
            .ConfigureLogging((hbc, logging) =>
            {
                logging.ClearProviders();
                logging.AddOpenTelemetry(options =>
                {
                    options.IncludeFormattedMessage = true;
                    options.IncludeScopes = true;
                    options.ParseStateValues = true;

                    options.AddConsoleExporter();
                });
            })
            .ConfigureServices((hbc, services) =>
            {
                services.AddSingleton<IKafkaProducerService, KafkaProducerService>();
                services.AddHostedService<SocialMediaCommentsWorker>();

                var kafkaSettings = hbc.Configuration.GetRequiredSection("KafkaSettings").Get<KafkaSettings>();
                var commentsProducerSettings = hbc.Configuration.GetRequiredSection("CommentsProducerSettings").Get<TopicSettings>();

                services.AddKafka(kafka => kafka
                    .UseMicrosoftLog()
                    .AddCluster(cluster => cluster
                    .WithBrokers(kafkaSettings?.BootstrapServers)
                    .WithSchemaRegistry(schema =>
                    {
                        schema.Url = kafkaSettings?.SchemaRegistry;
                        schema.BasicAuthCredentialsSource = Confluent.SchemaRegistry.AuthCredentialsSource.UserInfo;
                        schema.BasicAuthUserInfo = $"{kafkaSettings?.SaslUserName}:{kafkaSettings?.SaslPassword}";
                    })
                    .WithSecurityInformation(information =>
                    {
                        information.SecurityProtocol = SecurityProtocol.SaslSsl;
                        information.SaslMechanism = SaslMechanism.ScramSha256;
                        information.SaslUsername = kafkaSettings?.SaslUserName;
                        information.SaslPassword = kafkaSettings?.SaslPassword;
                    })
                    .AddProducer(
                        "social-media-comments-producer",
                        producer => producer
                            .DefaultTopic(commentsProducerSettings?.Topic)
                            .AddMiddlewares(m => m.AddSingleTypeSerializer<SocialMediaCommentEvent, NewtonsoftJsonSerializer>()))
                    ).AddOpenTelemetryInstrumentation());
            }).Build();

        _logger = appHost.Services.GetRequiredService<ILogger<Program>>();

        _logger.LogInformation("App Host created successfully");

        await appHost.RunAsync();
    }
}