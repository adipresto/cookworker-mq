using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace KitchenWpf;

// ponytail: single service file ports all of tui.go, no MVVM framework
public record QStat(
    [property: JsonPropertyName("name")] string Name = "",
    [property: JsonPropertyName("messages_ready")] int Ready = 0,
    [property: JsonPropertyName("messages_unacknowledged")] int Unack = 0,
    [property: JsonPropertyName("messages")] int Total = 0);

public record OrderItem(string Id, string Value)
{
    public string Display => string.IsNullOrEmpty(Value) ? ShortId(Id) : $"\"{Value}\" ({ShortId(Id)})";
    public static string ShortId(string id) => id.Length > 18 ? "…" + id.Substring(id.Length - 15) : id;
}

public class CookStat
{
    public string Pod { get; set; } = "";
    public string Full { get; set; } = "";
    public string Line { get; set; } = "";
    public string State { get; set; } = "idle";
    public string Order { get; set; } = "";
    public string Value { get; set; } = "";
    public DateTime Start { get; set; }
    public TimeSpan Prep { get; set; }

    // Status renders the countdown live from kitchen.state events (no log parsing).

    public string Status()
    {
        if (State == "cooking")
        {
            var left = Prep - (DateTime.Now - Start.ToLocalTime());
            if (left < TimeSpan.Zero) left = TimeSpan.Zero;
            return $"cooking \"{(!string.IsNullOrEmpty(Value) ? Value : Order)}\" · {(int)left.TotalSeconds}s left";
        }
        if (string.IsNullOrEmpty(Line)) return "(starting…)";
        if (Line.Contains("cooked")) return "idle";
        return Line.Length > 90 ? Line[..89] + "…" : Line;
    }
    public override string ToString() => $"{Pod}: {Status()}";
}

public class Snapshot
{
    public QStat Requests { get; set; } = new();
    public QStat Orders { get; set; } = new();
    public QStat Done { get; set; } = new();
    public List<CookStat> Cooks { get; set; } = [];
    public List<string> Waiter { get; set; } = [];
    public List<OrderItem> Waiting { get; set; } = [];
    public List<OrderItem> Ready { get; set; } = [];
    public List<OrderItem> Alerts { get; set; } = [];
    public int Want { get; set; }
    public string ReadyN { get; set; } = "";
    public DateTime At { get; set; } = DateTime.Now;
    public string Error { get; set; } = "";
}

public record StateEvent(
    [property: JsonPropertyName("pod")] string Pod = "",
    [property: JsonPropertyName("role")] string Role = "",
    [property: JsonPropertyName("state")] string State = "",
    [property: JsonPropertyName("order")] string Order = "",
    [property: JsonPropertyName("value")] string Value = "",
    [property: JsonPropertyName("prep")] string Prep = "",
    [property: JsonPropertyName("at")] string At = "");

public class KitchenService(string api, string user, string pass, string amqpUrl)
{
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(5) };
    private readonly object _stateLock = new();
    private readonly Dictionary<string, CookStat> _stateCooks = new();
    private readonly List<string> _stateWaiter = [];
    private int _stateWant;

    private HttpRequestMessage Req(HttpMethod m, string path, HttpContent? c = null)
    {
        var r = new HttpRequestMessage(m, api.TrimEnd('/') + path);
        r.Headers.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.ASCII.GetBytes($"{user}:{pass}")));
        if (c != null) r.Content = c;
        return r;
    }

    public async Task<QStat> GetQueueAsync(string q)
    {
        using var r = await _http.SendAsync(Req(HttpMethod.Get, $"/api/queues/%2F/{q}"));
        var body = await r.Content.ReadAsStringAsync();
        r.EnsureSuccessStatusCode();
        return JsonSerializer.Deserialize<QStat>(body) ?? new();
    }

    public async Task<List<OrderItem>> PeekQueueAsync(string q, int count)
    {
        var payload = JsonSerializer.Serialize(new { count, ackmode = "ack_requeue_true", encoding = "auto", truncate = 50000 });
        using var r = await _http.SendAsync(Req(HttpMethod.Post, $"/api/queues/%2F/{q}/get",
            new StringContent(payload, Encoding.UTF8, "application/json")));
        if (!r.IsSuccessStatusCode) return [];
        var raw = await r.Content.ReadAsStringAsync();
        var msgs = JsonSerializer.Deserialize<List<PeekMsg>>(raw) ?? [];
        var out_ = new List<OrderItem>();
        foreach (var m in msgs)
        {
            try
            {
                var o = JsonSerializer.Deserialize<PeekPayload>(m.Payload ?? "");
                if (o?.Id != null) out_.Add(new OrderItem(o.Id, o.Value ?? ""));
            }
            catch { /* skip bad payload */ }
        }
        return out_;
    }

    // State reads come from kitchen.state fanout (see StartStateListener).
    // KctlAsync below is ACTIONS only: writes still need kubectl.
    public async Task<Snapshot> FetchAsync()
    {
        var s = new Snapshot { At = DateTime.Now };
        var errs = new List<string>();
        foreach (var q in new[] { "requests", "orders", "done" })
        {
            try
            {
                var st = await GetQueueAsync(q);
                if (q == "requests") s.Requests = st;
                else if (q == "orders") s.Orders = st;
                else s.Done = st;
            }
            catch (Exception ex) { errs.Add($"{q}: {ex.Message}"); }
        }
        lock (_stateLock)
        {
            s.Cooks = _stateCooks.Values.ToList();
            s.Waiter = [.. _stateWaiter];
            s.Want = _stateWant == 0 ? _stateCooks.Count : _stateWant;
            s.ReadyN = $"{_stateCooks.Count}/{s.Want}";
        }
        try
        {
            s.Waiting = await PeekQueueAsync("orders", 20);
            s.Ready = await PeekQueueAsync("done", 20);
            s.Alerts = await PeekQueueAsync("notify-tui", 10);
        }
        catch { /* peek is best-effort */ }
        if (s.Cooks.Count == 0 && s.Waiter.Count == 0) errs.Add("state: waiting for kitchen.state events…");
        s.Error = string.Join(" · ", errs);
        return s;
    }

    public void SetWant(int want) { lock (_stateLock) { _stateWant = want; } }

    public void ApplyState(StateEvent e)
    {
        if (string.IsNullOrEmpty(e.Pod)) return;
        if (!DateTime.TryParseExact(e.At, "yyyy-MM-dd HH:mm:ss", null,
            System.Globalization.DateTimeStyles.None, out var at)) at = DateTime.Now;
        lock (_stateLock)
        {
            if (e.Role == "cook")
            {
                if (!_stateCooks.TryGetValue(e.Pod, out var c))
                {
                    c = new CookStat { Pod = ShortPod(e.Pod), Full = e.Pod };
                    _stateCooks[e.Pod] = c;
                }
                if (e.State == "cooking")
                {
                    c.State = "cooking"; c.Order = e.Order ?? ""; c.Value = e.Value ?? ""; c.Start = at;
                    c.Prep = ParseGoDuration(e.Prep ?? ""); c.Line = "cooking";
                }
                else { c.State = "idle"; c.Line = "cooked " + (e.Order ?? ""); }
                if (_stateWant == 0) _stateWant = _stateCooks.Count;
            }
            else if (e.Role == "waiter")
            {
                var line = e.At + " " + e.State;
                if (!string.IsNullOrEmpty(e.Value)) line += " " + e.Value;
                else if (!string.IsNullOrEmpty(e.Order)) line += " " + OrderItem.ShortId(e.Order);
                _stateWaiter.Add(line);
                while (_stateWaiter.Count > 6) _stateWaiter.RemoveAt(0);
            }
        }
    }

    // Go durations like "45s", "1m30s". TimeSpan.TryParse can't read them.
    private static TimeSpan ParseGoDuration(string s)
    {
        var m = System.Text.RegularExpressions.Regex.Match(s.Trim(),
            @"^(?:(\d+)h)?(?:(\d+)m(?!s))?(?:([\d.]+)s)?$");
        if (!m.Success) return TimeSpan.Zero;
        double h = m.Groups[1].Success ? double.Parse(m.Groups[1].Value) : 0;
        double min = m.Groups[2].Success ? double.Parse(m.Groups[2].Value) : 0;
        double sec = m.Groups[3].Success ? double.Parse(m.Groups[3].Value) : 0;
        if (h == 0 && min == 0 && sec == 0) return TimeSpan.Zero;
        return TimeSpan.FromHours(h) + TimeSpan.FromMinutes(min) + TimeSpan.FromSeconds(sec);
    }

    // Subscribes to kitchen.state fanout, no kubectl, no wsl. Reconnects on blips.
    // ponytail: fire-and-forget listener, reconnect loop if broker restarts matter
    public void StartStateListener(Action onEvent)
    {
        _ = Task.Run(async () =>
        {
            while (true)
            {
                try
                {
                    var factory = new ConnectionFactory { Uri = new Uri(amqpUrl) };
                    using var conn = factory.CreateConnection();
                    using var ch = conn.CreateModel();
                    ch.ExchangeDeclare("kitchen.state", "fanout", durable: true);
                    var q = ch.QueueDeclare("", durable: false, exclusive: true, autoDelete: true);
                    ch.QueueBind(q.QueueName, "kitchen.state", "");
                    var consumer = new RabbitMQ.Client.Events.EventingBasicConsumer(ch);
                    consumer.Received += (_, args) =>
                    {
                        try
                        {
                            var e = JsonSerializer.Deserialize<StateEvent>(args.Body.ToArray());
                            if (e != null) { ApplyState(e); onEvent(); }
                        }
                        catch { /* skip bad event */ }
                    };
                    ch.BasicConsume(q.QueueName, autoAck: true, consumer: consumer);
                    await Task.Delay(Timeout.InfiniteTimeSpan);
                }
                catch { await Task.Delay(TimeSpan.FromSeconds(5)); }
            }
        });
    }

    // kctl runs kubectl in WSL. ACTIONS only (kill/scale/restart writes);
    // state reads come from kitchen.state fanout, never kubectl.

    public static async Task<(string action, string error)> KctlAsync(string note, params string[] args)
    {
        var full = new[] { "-d", "NixOS", "--", "sudo", "kubectl", "--kubeconfig",
            "/etc/rancher/k3s/k3s.yaml", "-n", "kitchen" }.Concat(args).ToArray();
        var (code, out_) = await RunAsync("wsl", full);
        return code == 0 ? (note, "") : ("", out_.Trim());
    }

    private static async Task<(int code, string output)> RunAsync(string exe, string[] args)
    {
        var psi = new ProcessStartInfo(exe) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        var output = await p.StandardOutput.ReadToEndAsync() + await p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync();
        return (p.ExitCode, output);
    }

    private static string ShortPod(string p) => p.Contains('-') ? p[(p.LastIndexOf('-') + 1)..] : p;

    private ConnectionFactory Factory() => new() { Uri = new Uri(amqpUrl) };

    public void SendRequest(string value)
    {
        value = value.Trim();
        if (value == "") throw new ArgumentException("empty order");
        using var conn = Factory().CreateConnection();
        using var ch = conn.CreateModel();
        ch.QueueDeclare("requests", true, false, false);
        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new { value }));
        var props = ch.CreateBasicProperties();
        props.Persistent = true; props.ContentType = "application/json";
        ch.BasicPublish("", "requests", props, body);
    }

    public void Broadcast(string exchange, string value)
    {
        value = value.Trim();
        if (value == "") throw new ArgumentException("empty notice");
        using var conn = Factory().CreateConnection();
        using var ch = conn.CreateModel();
        var body = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new { id = $"notice-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}", value }));
        var props = ch.CreateBasicProperties();
        props.Persistent = true; props.ContentType = "application/json";
        ch.BasicPublish(exchange, "", props, body);
    }

    private record PeekMsg([property: JsonPropertyName("payload")] string? Payload);
    private record PeekPayload([property: JsonPropertyName("id")] string? Id, [property: JsonPropertyName("value")] string? Value);
}
