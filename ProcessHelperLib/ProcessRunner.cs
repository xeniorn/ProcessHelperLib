using System.Collections.Concurrent;
using System.Diagnostics;

namespace ProcessHelperLib;

public static class ProcessRunner
{
    public static async Task<bool> RunPowerShellCommand(string cmd, CancellationToken? cancellationTokenInput = null)
    {
        var cancellationToken = cancellationTokenInput ?? CancellationToken.None;

        var res = 1;

        await foreach (var chunk in RunProcessAsync("powershell", cmd, true,
                               cancellationToken)
                           .WithCancellation(cancellationToken))
        {
            if (chunk.Type == AsyncProcessRunnerResponseChunkType.ExitCode)
            {
                res = int.Parse(chunk.Content);
            };
        }

        return (res == 0);
    }

    /// <summary>
    /// Runs the target process in an async manner, and keeps yielding typed outcomes (output, error, or exit code).
    /// </summary>
    /// <param name="program"></param>
    /// <param name="argsString"></param>
    /// <param name="useShell"></param>
    /// <param name="cancellationTokenInput"></param>
    /// <returns></returns>
    public static async IAsyncEnumerable<AsyncProcessRunnerResponseChunk> RunProcessAsync(string program,
        string argsString,
        bool useShell = false,
        CancellationToken? cancellationTokenInput = null)
    {
        const int checkPeriodMs = 100;
        var cancellationToken = cancellationTokenInput ?? CancellationToken.None;

        var msgQueue = new ConcurrentQueue<AsyncProcessRunnerResponseChunk>();

        void OutputReceivedHandler(object sender, DataReceivedEventArgs args)
        {
            if (args.Data is not null) msgQueue.Enqueue(new AsyncProcessRunnerResponseChunk(AsyncProcessRunnerResponseChunkType.OutputStream, $"{args.Data}\n"));
        }

        void ErrorReceivedHandler(object sender, DataReceivedEventArgs args)
        {
            if (args.Data is not null) msgQueue.Enqueue(new AsyncProcessRunnerResponseChunk(AsyncProcessRunnerResponseChunkType.ErrorStream, $"{args.Data}\n"));
        }

        var task = RunProcessAsync(program, 
            argsString, 
            OutputReceivedHandler, 
            ErrorReceivedHandler, 
            useShell,
            cancellationTokenInput);
        
        while (true)
        {
            if (cancellationToken.IsCancellationRequested) yield break;

            while (msgQueue.TryDequeue(out var result))
            {
                if (cancellationToken.IsCancellationRequested) yield break;
                yield return result!;
            }

            try
            {
                await Task.Delay(checkPeriodMs, cancellationToken);
            }
            catch (TaskCanceledException)
            {
                yield break;
            }
            
            
            if (!task.IsCompleted) continue;
            
            var exitCode = task.Result;
            yield return new AsyncProcessRunnerResponseChunk(AsyncProcessRunnerResponseChunkType.ExitCode, exitCode.ToString());
            break;
        }

        while (msgQueue.TryDequeue(out var result))
        {
            if (cancellationToken.IsCancellationRequested) yield break;
            yield return result!;
        }
    }

    /// <summary>
    /// Runs the target process in an async manner, allowing the caller to subscribe to stdout / stderr received events.
    /// </summary>
    /// <param name="program"></param>
    /// <param name="argsString"></param>
    /// <param name="useShell"></param>
    /// <param name="cancellationTokenInput"></param>
    /// <returns></returns>
    public static async Task<int> RunProcessAsync(string program,
        string argsString,
        DataReceivedEventHandler? outputDataReceivedHandler,
        DataReceivedEventHandler? errorDataReceivedHandler,
        bool useShell = false,
        CancellationToken? cancellationTokenInput = null)
    {
        var cancellationToken = cancellationTokenInput ?? CancellationToken.None;

        var startInfo = new ProcessStartInfo()
        {
            FileName = program,
            Arguments = argsString,
            CreateNoWindow = true,
            UseShellExecute = useShell,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };

        var proc = new Process()
        {
            StartInfo = startInfo,
        };

        if (errorDataReceivedHandler is not null)
        {
            proc.ErrorDataReceived += errorDataReceivedHandler;
        }

        if (outputDataReceivedHandler is not null)
        {
            proc.OutputDataReceived += outputDataReceivedHandler;
        }

        proc.Start();

        proc.BeginErrorReadLine();
        proc.BeginOutputReadLine();

        await proc.WaitForExitAsync(cancellationToken);

        if (cancellationToken.IsCancellationRequested)
        {
            proc.CancelErrorRead();
            proc.CancelOutputRead();
            proc.Kill();
            return (CancelledProcessExitCode);
        }

        return (proc.ExitCode);
    }

    public const int CancelledProcessExitCode = int.MinValue;


    public static async Task<(int ExitCode, List<string> outdata, List<string> errdata)> RunProcessAsync_GetSeparateOutputsAsLists(
        string program,
        string argsString,
        bool useShell = false,
        CancellationToken? cancellationTokenInput = null)
    {
        var cancellationToken = cancellationTokenInput ?? CancellationToken.None;

        var outdata = new List<string>();
        var errdata = new List<string>();

        DataReceivedEventHandler? ErrorDataReceivedH = delegate (object sender, DataReceivedEventArgs args) { if (args.Data is not null) outdata.Add(args.Data); };
        DataReceivedEventHandler? OutputDataReceivedH = delegate (object sender, DataReceivedEventArgs args) { if (args.Data is not null) outdata.Add(args.Data); };

        var res = await RunProcessAsync(
            program,
            argsString,
            OutputDataReceivedH,
            ErrorDataReceivedH,
            useShell,
            cancellationToken);

        return (res, outdata, errdata);
    }


    public static async Task<(int ExitCode, List<string> outdata)> RunProcessAsync_GetCombinedOutputAsList(
        string program, 
        string argsString, 
        bool useShell = false, 
        CancellationToken? cancellationTokenInput = null)
    {
        var cancellationToken = cancellationTokenInput ?? CancellationToken.None;

        var outdata = new List<string>();

        DataReceivedEventHandler? ErrorDataReceivedH = delegate (object sender, DataReceivedEventArgs args) { if (args.Data is not null) outdata.Add(args.Data); };
        DataReceivedEventHandler? OutputDataReceivedH = delegate (object sender, DataReceivedEventArgs args) { if (args.Data is not null) outdata.Add(args.Data); };

        var res = await RunProcessAsync(
            program,
            argsString,
            OutputDataReceivedH,
            ErrorDataReceivedH,
            useShell,
            cancellationToken);

        return (res, outdata);
    }
}