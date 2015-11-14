

/**
 * HeartBeat State associated with any given endpoint.
 */
class HeartBeatState
{
    private int generation;
    private int version;

    HeartBeatState(int gen)
    {
        this(gen, 0);
    }

    void updateHeartBeat()
    {
        version = VersionGenerator.getNextVersion();
    }

    int getHeartBeatVersion()
    {
        return version;
    }

    void forceNewerGenerationUnsafe()
    {
        generation += 1;
    }

    void forceHighestPossibleVersionUnsafe()
    {
        version = Integer.MAX_VALUE;
    }
}

