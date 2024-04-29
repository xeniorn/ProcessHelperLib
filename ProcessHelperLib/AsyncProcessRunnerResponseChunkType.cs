namespace ProcessHelperLib;

public enum AsyncProcessRunnerResponseChunkType
{
    Unknown,
    ExitCode,
    OutputStream,
    ErrorStream
}