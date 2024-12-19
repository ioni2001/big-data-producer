using Big.Data.DataProducer.Models.Events;

namespace Big.Data.DataProducer.Services;

public interface ICsvParserService
{
    List<SocialMediaCommentEvent> GetCommentData();
}
