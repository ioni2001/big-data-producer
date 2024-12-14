namespace Big.Data.DataProducer.Models.Events;

public class SocialMediaCommentEvent
{
    public required string Name { get; set; }

    public required string Comment { get; set; }
}
