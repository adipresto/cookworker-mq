using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using System.Windows.Threading;

namespace KitchenWpf;

public partial class MainWindow : Window
{
    private readonly KitchenService _svc;
    private readonly DispatcherTimer _timer = new() { Interval = TimeSpan.FromSeconds(3) };
    private Snapshot _cur = new();
    private bool _busy;

    public MainWindow()
    {
        InitializeComponent();
        _svc = new KitchenService(
            Env("RABBIT_API", "http://localhost:15672"),
            Env("RABBIT_USER", "guest"),
            Env("RABBIT_PASS", "guest"),
            Env("AMQP_URL", "amqp://guest:guest@localhost:5672/"));
        _timer.Tick += async (_, _) => await RefreshAsync();
        _timer.Start();
        _svc.StartStateListener(() => Dispatcher.InvokeAsync(RefreshAsync));
        Loaded += async (_, _) => await RefreshAsync();
        KeyDown += OnKey;
    }

    private static string Env(string k, string def) =>
        string.IsNullOrEmpty(Environment.GetEnvironmentVariable(k)) ? def : Environment.GetEnvironmentVariable(k)!;

    private void OnKey(object sender, KeyEventArgs e)
    {
        if (OrderBox.IsFocused) return; // typing
        switch (e.Key)
        {
            case Key.R when Keyboard.Modifiers.HasFlag(ModifierKeys.Shift): _ = RestartAsync(); break;
            case Key.R: _ = RefreshAsync(); break;
            case Key.X when Keyboard.Modifiers.HasFlag(ModifierKeys.Shift): _ = KillAllAsync(); break;
            case Key.X: _ = KillAsync(); break;
            case Key.OemPlus: _ = ScaleAsync(1); break;
            case Key.OemMinus: _ = ScaleAsync(-1); break;
        }
    }

    private async Task RefreshAsync()
    {
        if (_busy) return;
        _busy = true; LoadingText.Visibility = Visibility.Visible;
        try
        {
            _cur = await _svc.FetchAsync();
            UpdatedText.Text = $"updated {_cur.At:HH:mm:ss}";
            QueuesText.Text = $"{QLine(_cur.Requests)}\n{QLine(_cur.Orders)}\n{QLine(_cur.Done)}";
            WaitingList.ItemsSource = _cur.Waiting.Select(o => o.Display).ToList()
                .AppendExtra(_cur.Orders.Ready - _cur.Waiting.Count);
            CookingList.ItemsSource = _cur.Cooks.Where(c => c.State == "cooking")
                .Select(c => $"\"{(string.IsNullOrEmpty(c.Value) ? c.Order : c.Value)}\" ({OrderItem.ShortId(c.Order)}) @{c.Pod}").ToList()
                .AppendExtra(_cur.Orders.Unack - _cur.Cooks.Count(c => c.State == "cooking"));
            ReadyList.ItemsSource = _cur.Ready.Select(o => o.Display).ToList()
                .AppendExtra(_cur.Done.Ready - _cur.Ready.Count);
            AlertsList.ItemsSource = _cur.Alerts.Count == 0 ? ["(none)"] : _cur.Alerts.Select(o => o.Display).ToList();
            DeployText.Text = string.IsNullOrEmpty(_cur.ReadyN) ? "cook deploy" : $"cook deploy: {_cur.ReadyN} ready (want {_cur.Want})";
            var sel = CooksList.SelectedIndex;
            CooksList.ItemsSource = _cur.Cooks.Select(c => c.ToString()).ToList();
            if (CooksList.Items.Count > 0) CooksList.SelectedIndex = Math.Clamp(sel < 0 ? 0 : sel, 0, CooksList.Items.Count - 1);
            WaiterText.Text = _cur.Waiter.Count == 0 ? "(nothing yet)" : string.Join("\n", _cur.Waiter);
            ErrorText.Text = string.IsNullOrEmpty(_cur.Error) ? "" : "! " + _cur.Error;
        }
        catch (Exception ex) { ErrorText.Text = "! " + ex.Message; }
        finally { _busy = false; LoadingText.Visibility = Visibility.Collapsed; }
    }

    private static string QLine(QStat s) => string.IsNullOrEmpty(s.Name) ? "…" : $"{s.Name,-8} Ready={s.Ready,-4} Unacked={s.Unack,-4}";

    private void Notice(string m, bool ok) { NoticeText.Text = m; NoticeText.Foreground = ok ? System.Windows.Media.Brushes.Green : System.Windows.Media.Brushes.Red; }

    private void Order_Click(object s, RoutedEventArgs e) => SendOrder();
    private void Menu_Click(object s, RoutedEventArgs e) => SendBroadcast("menu.update");
    private void Alarm_Click(object s, RoutedEventArgs e) => SendBroadcast("shop.alarm");
    private void RefreshBtn_Click(object s, RoutedEventArgs e) => _ = RefreshAsync();
    private async void Kill_Click(object s, RoutedEventArgs e) => await KillAsync();
    private async void KillAll_Click(object s, RoutedEventArgs e) => await KillAllAsync();
    private async void Restart_Click(object s, RoutedEventArgs e) => await RestartAsync();
    private async void ScaleUp_Click(object s, RoutedEventArgs e) => await ScaleAsync(1);
    private async void ScaleDown_Click(object s, RoutedEventArgs e) => await ScaleAsync(-1);

    private void SendOrder()
    {
        try { _svc.SendRequest(OrderBox.Text); Notice($"requested {OrderBox.Text.Trim()}", true); OrderBox.Clear(); }
        catch (Exception ex) { Notice("send failed: " + ex.Message, false); }
        _ = RefreshAsync();
    }

    private void SendBroadcast(string exchange)
    {
        try { _svc.Broadcast(exchange, OrderBox.Text); Notice($"broadcast {OrderBox.Text.Trim()}", true); OrderBox.Clear(); }
        catch (Exception ex2) { Notice("send failed: " + ex2.Message, false); }
        _ = RefreshAsync();
    }

    private async Task KillAsync()
    {
        if (CooksList.SelectedIndex < 0 || CooksList.SelectedIndex >= _cur.Cooks.Count) { Notice("no cooks to kill", false); return; }
        var c = _cur.Cooks[CooksList.SelectedIndex];
        await RunKctl($"killed {c.Pod}", "delete", $"pod/{c.Full}", "--wait=false");
    }

    private async Task KillAllAsync() => await RunKctl("killed all cooks", "delete", "pods", "-l", "app=cook", "--wait=false");

    private async Task ScaleAsync(int delta)
    {
        var want = _cur.Want + delta;
        if (want is < 0 or > 5) { Notice(want > 5 ? "already at max 5 cooks" : "already at 0 cooks", false); return; }
        _svc.SetWant(want);
        await RunKctl(delta > 0 ? "scaled up" : "scaled down", "scale", "deploy/cook", $"--replicas={want}");
    }

    private async Task RestartAsync() => await RunKctl("restarted cooks", "rollout", "restart", "deploy/cook");

    private async Task RunKctl(string note, params string[] args)
    {
        var (ok, err) = await KitchenService.KctlAsync(note, args);
        Notice(string.IsNullOrEmpty(err) ? ok : "action failed: " + err, string.IsNullOrEmpty(err));
        await RefreshAsync();
    }
}

static file class ListExt
{
    public static List<string> AppendExtra(this List<string> l, int extra)
    {
        if (l.Count == 0) l = ["(none)"];
        if (extra > 0) l.Add($"+{extra} more…");
        return l;
    }
}
