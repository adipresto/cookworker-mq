# Graph Report - worker-mq  (2026-10-08)

## Corpus Check
- Corpus is ~3,725 words - fits in a single context window. You may not need a graph.

## Summary
- 77 nodes · 131 edges · 13 communities (9 shown, 4 thin omitted)
- Extraction: 98% EXTRACTED · 2% INFERRED · 0% AMBIGUOUS · INFERRED: 3 edges (avg confidence: 0.88)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- TUI messages and imports
- Worker runtime dependencies
- TUI update loop
- Kitchen state polling
- Cook and waiter roles
- TUI rendering
- Deploy manifests and CI
- Order publishing
- Queue stats API
- Duration config
- Order peeking
- Go module

## God Nodes (most connected - your core abstractions)
1. `snapshot` - 8 edges
2. `fetch()` - 8 edges
3. `model` - 8 edges
4. `runWaiter()` - 7 edges
5. `cookStat` - 7 edges
6. `publishOrder()` - 6 edges
7. `main()` - 6 edges
8. `runCook()` - 5 edges
9. `main()` - 5 edges
10. `orderLine()` - 5 edges

## Surprising Connections (you probably didn't know these)
- `CI pipeline (vet, build, push ghcr.io/adipresto/cookworker-mq:main)` --references--> `cook Deployment (2 replicas, ROLE=cook)`  [INFERRED]
  .github/workflows/ci.yml → k8s/cook.yaml
- `CI pipeline (vet, build, push ghcr.io/adipresto/cookworker-mq:main)` --references--> `waiter Deployment (Recreate, ROLE=waiter, single owner of done)`  [INFERRED]
  .github/workflows/ci.yml → k8s/waiter.yaml
- `cook Deployment (2 replicas, ROLE=cook)` --shares_data_with--> `waiter Deployment (Recreate, ROLE=waiter, single owner of done)`  [INFERRED]
  k8s/cook.yaml → k8s/waiter.yaml
- `cook Deployment (2 replicas, ROLE=cook)` --references--> `kitchen Namespace`  [EXTRACTED]
  k8s/cook.yaml → k8s/ns.yaml
- `waiter Deployment (Recreate, ROLE=waiter, single owner of done)` --references--> `kitchen Namespace`  [EXTRACTED]
  k8s/waiter.yaml → k8s/ns.yaml

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Kitchen deployment set** — k8s_cook_cook_deployment, k8s_waiter_waiter_deployment, k8s_ns_kitchen_namespace [INFERRED 0.85]

## Communities (13 total, 4 thin omitted)

### Community 0 - "TUI messages and imports"
Cohesion: 0.14
Nodes (13): go_pkg_bytes, go_pkg_flag, go_pkg_github_com_charmbracelet_bubbles_textinput, go_pkg_github_com_charmbracelet_bubbletea, go_pkg_github_com_charmbracelet_lipgloss, go_pkg_io, go_pkg_net_http, go_pkg_os_exec (+5 more)

### Community 1 - "Worker runtime dependencies"
Cohesion: 0.15
Nodes (12): go_pkg_context, go_pkg_encoding_json, go_pkg_fmt, go_pkg_github_com_rabbitmq_amqp091_go, go_pkg_log, go_pkg_math_rand, go_pkg_os, go_pkg_os_signal (+4 more)

### Community 2 - "TUI update loop"
Cohesion: 0.25
Nodes (7): github.com/charmbracelet/bubbles/textinput.Model, github.com/charmbracelet/bubbletea.Cmd, github.com/charmbracelet/bubbletea.Model, github.com/charmbracelet/bubbletea.Msg, kctl(), sendRequest(), model

### Community 3 - "Kitchen state polling"
Cohesion: 0.36
Nodes (7): time.Time, fetch(), getK8s(), shortPod(), cookStat, dataMsg, snapshot

### Community 4 - "Cook and waiter roles"
Cohesion: 0.48
Nodes (7): github.com/rabbitmq/amqp091-go.Connection, declare(), dial(), env(), main(), runCook(), runWaiter()

### Community 5 - "TUI rendering"
Cohesion: 0.53
Nodes (4): getenv(), main(), orderLine(), shortID()

### Community 6 - "Deploy manifests and CI"
Cohesion: 0.83
Nodes (4): CI pipeline (vet, build, push ghcr.io/adipresto/cookworker-mq:main), cook Deployment (2 replicas, ROLE=cook), kitchen Namespace, waiter Deployment (Recreate, ROLE=waiter, single owner of done)

### Community 7 - "Order publishing"
Cohesion: 0.67
Nodes (4): github.com/rabbitmq/amqp091-go.Channel, publish(), publishOrder(), Order

### Community 8 - "Queue stats API"
Cohesion: 0.67
Nodes (3): getQueue(), qline(), qstat

## Knowledge Gaps
- **5 isolated node(s):** `github.com/adipresto/cookworker-mq`, `Request`, `tickMsg`, `sentMsg`, `actionMsg`
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 24 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **4 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `model` connect `TUI update loop` to `TUI messages and imports`, `Kitchen state polling`, `TUI rendering`?**
  _High betweenness centrality (0.108) - this node is a cross-community bridge._
- **Why does `cookStat` connect `Kitchen state polling` to `TUI messages and imports`, `Duration config`, `TUI rendering`?**
  _High betweenness centrality (0.051) - this node is a cross-community bridge._
- **Why does `kctl()` connect `TUI update loop` to `TUI messages and imports`?**
  _High betweenness centrality (0.041) - this node is a cross-community bridge._
- **What connects `github.com/adipresto/cookworker-mq`, `Request`, `tickMsg` to the rest of the system?**
  _5 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `TUI messages and imports` be split into smaller, more focused modules?**
  _Cohesion score 0.14285714285714285 - nodes in this community are weakly interconnected._