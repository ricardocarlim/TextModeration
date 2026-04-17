using TextMediation.Worker.Models;

namespace TextMediation.Worker.Services;

public interface IModerationService
{
    ModeratedComment Classify(string commentId, string text);
}
