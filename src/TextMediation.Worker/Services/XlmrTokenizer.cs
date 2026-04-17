using Microsoft.ML.Tokenizers;

public sealed class XlmrTokenizer
{
    private const int BosId = 0;
    private const int PadId = 1;
    private const int EosId = 2;
    private const int UnkId = 3;
    private const int FairseqOffset = 1;

    private readonly SentencePieceTokenizer _sp;
    private readonly int _maxSeqLen;

    public XlmrTokenizer(string modelPath, int maxSeqLen)
    {
        using var fs = File.OpenRead(modelPath);
        _sp = SentencePieceTokenizer.Create(fs);
        _maxSeqLen = maxSeqLen;
    }

    public (long[] InputIds, long[] AttentionMask) Encode(string text)
    {
        IReadOnlyList<int> spIds = _sp.EncodeToIds(text);

        int budget = _maxSeqLen - 2;
        int take = Math.Min(spIds.Count, budget);

        var ids = new long[_maxSeqLen];
        var mask = new long[_maxSeqLen];

        ids[0] = BosId;
        mask[0] = 1;

        for (int i = 0; i < take; i++)
        {
            int sp = spIds[i];
            ids[i + 1] = sp == 0 ? UnkId : sp + FairseqOffset;
            mask[i + 1] = 1;
        }

        ids[take + 1] = EosId;
        mask[take + 1] = 1;

        for (int i = take + 2; i < _maxSeqLen; i++)
            ids[i] = PadId;

        return (ids, mask);
    }
}