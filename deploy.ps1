# Deploy worker-mq to WSL k3s. Idempotent: fresh install or refresh to :main.
# Usage: .\deploy.ps1 [-Distro NixOS] [-Uninstall]
param([string]$Distro = "NixOS", [switch]$Uninstall)

$ErrorActionPreference = "Stop"
function k { wsl -d $Distro -- sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml @args; if ($LASTEXITCODE) { throw "kubectl $($args -join ' ') failed" } }
$wslPath = "/mnt/" + $PSScriptRoot[0].ToString().ToLower() + $PSScriptRoot.Substring(2).Replace("\", "/")

if ($Uninstall) { k delete namespace kitchen --ignore-not-found; "removed"; return }

# Windows host IP as seen from WSL (gateway). No hardcoded IP.
$route = wsl -d $Distro -- sh -c 'ip route show default'
$HostIp = if ($route -match 'via ([\d.]+)') { $Matches[1] } else { "172.28.144.1" }
wsl -d $Distro -- sh -c "nc -vz -w 3 $HostIp 5672" | Out-Null
if ($LASTEXITCODE) { throw "broker unreachable at ${HostIp}:5672 - start RabbitMQ on Windows first" }

k apply -f "$wslPath/k8s/ns.yaml"
wsl -d $Distro -- sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml -n kitchen create secret generic mq --from-literal=AMQP_URL=amqp://guest:guest@${HostIp}:5672/ --dry-run=client -o yaml |
  wsl -d $Distro -- sudo kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml apply -f -
k apply -f "$wslPath/k8s/cook.yaml"
k apply -f "$wslPath/k8s/waiter.yaml"
k rollout status deploy/cook -n kitchen --timeout=120s
k rollout status deploy/waiter -n kitchen --timeout=120s
k get pods -n kitchen
