# cookworker-mq — cook on break, order can't be lost

Work queue (`orders` + `done` + `requests`), manual ack, Qos 1, persistent.
Waiter takes requests, cooks compete, dishes come back on `done`.

## Deploy after CI is green

CI pushes `ghcr.io/adipresto/cookworker-mq:main`. Pods use
`imagePullPolicy: Always`, so a restart pulls the fresh image.

```bash
# from Windows PowerShell; k3s lives in WSL/NixOS
wsl -d NixOS -- sh -c 'sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kitchen rollout restart deploy/cook deploy/waiter'
wsl -d NixOS -- sh -c 'sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kitchen get pods'
```

First install only (namespace, secret, deploys):

```bash
wsl -d NixOS -- sh -c 'sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml apply -f /mnt/c/Users/KAINE/Documents/Development/worker-mq/k8s/ns.yaml'
wsl -d NixOS -- sh -c 'sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml apply -f /mnt/c/Users/KAINE/Documents/Development/worker-mq/k8s/secret.yaml'
wsl -d NixOS -- sh -c 'sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml apply -f /mnt/c/Users/KAINE/Documents/Development/worker-mq/k8s/cook.yaml'
wsl -d NixOS -- sh -c 'sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml apply -f /mnt/c/Users/KAINE/Documents/Development/worker-mq/k8s/waiter.yaml'
```

Broker must be reachable first: `AMQP_URL=amqp://guest:guest@172.28.144.1:5672/`
(see `k8s/secret.yaml`). Verify from WSL:

```bash
wsl -d NixOS -- sh -c 'nc -vz -w 3 172.28.144.1 5672'
```

## Watch it

```powershell
cd C:\Users\KAINE\Documents\Development\worker-mq
go run tui.go          # live: queues, per-cook countdown, waiter log, order box
go run tui.go -once    # one snapshot, no TUI
```

Broker UI: `http://localhost:15672` (guest/guest) → Queues →
`orders`/`done`/`requests` (Ready vs Unacked).

## Input a new order

In the TUI: `o`, type the dish, `enter` → goes to the waiter via
`requests`, waiter publishes to `orders`. `esc` cancels typing,
`ctrl+u` clears the line.

Without the TUI (same path, through the waiter):

```powershell
curl -s -u guest:guest -H "content-type: application/json" -X POST http://localhost:15672/api/exchanges/%2F//publish -d '{"properties":{"delivery_mode":2},"routing_key":"requests","payload":"{\"value\":\"mie ayam\"}","payload_encoding":"string"}'
```

Change `id` thinking: the waiter assigns ids; duplicates are deduped
only on `done` by id, so every send cooks.

## Create chaos

Cooks take 15–90s per order — plenty of window to kill one mid-dish.

```bash
# one cook on break: sibling takes over, order requeues
wsl -d NixOS -- sh -c 'sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kitchen delete pods -l app=cook'
```

```bash
# both cooks down mid-flight: order waits in Ready, served when pods return
wsl -d NixOS -- sh -c 'sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kitchen rollout restart deploy/waiter'
# then within a minute:
wsl -d NixOS -- sh -c 'sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kitchen delete pods -l app=cook'
```

```powershell
# broker restart: Ready orders survive (durable queue + persistent msgs)
docker restart rabbitmq-rabbitmq-1
```

What "no loss" looks like: every `ordered`/`cooking` is eventually
followed by `served` with the same id — in TUI EVENTS, or:

```bash
wsl -d NixOS -- sh -c 'sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kitchen logs deploy/waiter --tail=10'
```
