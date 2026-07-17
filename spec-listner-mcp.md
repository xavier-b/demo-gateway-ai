# Spécification — Composant d'écoute des requêtes MCP (`McpListenerFilter`)

## 1. Contexte et objectif

La gateway (`demo-gateway-ai`, Spring Cloud Gateway / WebFlux) route du trafic vers des
serveurs MCP (ex. route `mcp-weather` → `http://localhost:8080`). Le protocole MCP
(Model Context Protocol) en transport **Streamable HTTP** repose sur :

- des **requêtes** HTTP `POST` portant un message **JSON-RPC 2.0** (`Content-Type: application/json`),
  de taille bornée en pratique (quelques Ko) ;
- des **réponses** soit `application/json` (réponse unique), soit `text/event-stream` (SSE),
  potentiellement **longues, infinies ou à livraison incrémentale** (streaming de tokens,
  notifications serveur, progress).

L'objectif du composant est d'**observer le trafic MCP au fil de l'eau** pour en extraire
la sémantique protocolaire (méthode JSON-RPC, outil appelé, identifiants de corrélation)
à des fins d'audit, de log, de métriques ou d'enrichissement du routage — **sans jamais
dégrader le caractère streaming de la réponse**.

Ce composant remplace l'ébauche `McpAnalysisFilter` (supprimée) et s'appuie sur les
patterns déjà présents dans le projet (`LoggingGlobalFilter`, `RequestLoggingWebFilter`).

## 2. Périmètre

### Inclus
- Interception des échanges sur les routes MCP de la gateway.
- Parsing du payload JSON-RPC des **requêtes entrantes** (avec bufferisation contrôlée).
- Observation **passive et non bufferisante** du flux de réponse (JSON ou SSE).
- Publication des informations extraites (attributs d'exchange, logs, hooks).

### Exclus
- Modification du payload (requête ou réponse) — le composant est en lecture seule.
- Blocage / filtrage de sécurité des appels MCP (pourra s'appuyer sur les attributs
  publiés, mais hors périmètre de cette spec).
- Support du transport MCP stdio (non pertinent pour une gateway HTTP).

## 3. Exigences fonctionnelles

### EF-1 — Ciblage des routes MCP
Le filtre ne traite que les échanges identifiés comme MCP. L'identification est
**configurable** et par défaut basée sur l'identifiant de route Spring Cloud Gateway
(liste de route ids, ex. `mcp-weather`) ou un préfixe de chemin (ex. `/mcp*`).
Tout autre trafic traverse le filtre sans aucun coût (pas de décoration, pas de copie).

### EF-2 — Bufferisation bornée de la requête
Pour les requêtes `POST` dont le `Content-Type` est compatible JSON
(`application/json`, paramètres charset acceptés) :

1. Le corps est agrégé en mémoire (`DataBufferUtils.join`) **avec une limite de taille
   configurable** (`mcp.listener.max-request-bytes`, défaut : 256 Ko).
2. Si la limite est dépassée, la gateway **interrompt la requête en cours** : la lecture
   du corps est annulée, les buffers déjà reçus sont libérés, la requête n'est **pas**
   transmise au serveur MCP et la gateway répond **`413 Payload Too Large`** avec un
   corps d'erreur JSON-RPC (`{"jsonrpc":"2.0","error":{"code":-32600,"message":
   "Request payload too large"},"id":null}`). Un avertissement est logué (taille reçue,
   limite, route).
3. Après lecture, le corps est **ré-exposé à l'identique** vers la chaîne de filtres
   (décorateur `ServerHttpRequestDecorator` ré-émettant les octets bufferisés), afin que
   le proxying vers le serveur MCP reçoive exactement le payload original.
4. Les `DataBuffer` consommés sont systématiquement libérés (`DataBufferUtils.release`)
   pour éviter toute fuite mémoire Netty.

Les méthodes sans corps (`GET` utilisé pour ouvrir un flux SSE, `DELETE` de session)
sont observées via leurs seuls headers/URI, sans bufferisation.

### EF-3 — Parsing JSON-RPC de la requête
Le payload bufferisé est parsé en JSON. Le composant extrait, quand présents :

| Champ | Source JSON-RPC | Exemple |
|---|---|---|
| `method` | `$.method` | `initialize`, `tools/call`, `tools/list`, `notifications/initialized` |
| `id` | `$.id` | corrélation requête/réponse (absent pour les notifications) |
| `toolName` | `$.params.name` (si `method == tools/call`) | `get_weather` |
| `protocolVersion` | `$.params.protocolVersion` (si `initialize`) | `2025-03-26` |
| `sessionId` | header `Mcp-Session-Id` | identifiant de session Streamable HTTP |

Le parsing doit aussi gérer le cas **batch** (tableau JSON de messages) : chaque élément
est analysé, et la liste des méthodes est exposée.

### EF-4 — Tolérance aux erreurs de parsing
Un JSON invalide, un charset inattendu, un message non JSON-RPC ou tout autre échec de
parsing ne doit **jamais** faire échouer l'échange : le filtre logue en `WARN` (payload
tronqué) et continue en pass-through avec le corps ré-émis à l'identique.

### EF-5 — Observation de la réponse au fil de l'eau, sans bufferisation
La réponse est décorée (`ServerHttpResponseDecorator`) pour être **observée chunk par
chunk** via `doOnNext`, sans `join`, sans agrégation, sans copie des octets :

- les `DataBuffer` sont inspectés **positionnellement** (ex. `buffer.toString(charset)`
  sans avancer l'index de lecture) puis transmis tels quels à l'écriture réseau ;
- `writeWith` **et** `writeAndFlushWith` sont décorés — ce dernier est indispensable
  pour le SSE, où chaque évènement doit être flushé immédiatement ;
- la contre-pression (backpressure) du flux est préservée : aucune opération de
  l'observateur ne doit demander plus d'éléments ni retarder la propagation
  (interdiction de `buffer()`, `collectList()`, `delayElements()`, appels bloquants) ;
- la **latence du premier octet** (TTFB) et le délai inter-chunks ajoutés doivent rester
  négligeables (cible : < 1 ms par chunk).

### EF-6 — Extraction légère côté réponse
Sur le flux de réponse, l'extraction se limite à ce qui est possible **sans état
inter-chunk coûteux** :

- `Content-Type` de la réponse (JSON vs `text/event-stream`) et code HTTP, logués une
  seule fois à la première écriture ;
- header `Mcp-Session-Id` retourné par le serveur lors de l'`initialize` ;
- comptage des octets et des chunks, durée totale du flux (métriques) ;
- **obligatoire** : détection des frontières d'évènements SSE pour extraire
  `$.result` / `$.error` / `id` des messages JSON-RPC de réponse, y compris quand un
  évènement est **coupé à cheval sur plusieurs chunks**. Deux niveaux de traitement :
  1. **Framing SSE** : machine à états par flux de réponse, qui découpe les lignes
     (`data:`, `event:`, `id:`, commentaires `:`) et détecte la fin d'évènement
     (ligne vide), en tolérant les coupures de chunk au milieu d'une ligne ;
  2. **Parsing JSON incrémental** : le payload `data:` est parsé via le parser
     **non-bloquant de Jackson 3** — support vérifié : `tools.jackson.core` expose
     `JsonFactory.createNonBlockingByteArrayParser(ObjectReadContext)` (variante
     `ByteBufferFeeder` disponible) ; les octets sont poussés au fil de l'eau via
     `parser.nonBlockingInputFeeder().feedInput(...)` et `nextToken()` retourne
     `JsonToken.NOT_AVAILABLE` tant que l'évènement est incomplet. Un évènement coupé
     entre deux chunks est donc parsé sans retenir les `DataBuffer` originaux : seuls
     l'état interne du parser et les champs extraits sont conservés.

  L'accumulation reste **bornée** : au plus `mcp.listener.max-sse-event-bytes`
  (défaut 16 Ko) par évènement en cours. Au-delà, l'évènement est abandonné (parser
  réinitialisé, `WARN` logué) et le flux continue d'être proxifié normalement — on ne
  peut pas répondre en erreur au milieu d'un stream déjà entamé. ⚠ Cette limite doit
  être appliquée **par le composant lui-même** : les `StreamReadConstraints`
  (`maxDocumentLength`) ne sont pas appliquées de façon fiable par les parsers async
  de Jackson 3.x (cf. avis GHSA-2m67-wjpj-xhg9).

### EF-7 — Publication des informations extraites
Les données extraites de la requête sont publiées **avant** l'appel au backend pour être
exploitables par les filtres aval :

- attributs d'exchange : `mcp.method`, `mcp.id`, `mcp.toolName`, `mcp.sessionId`,
  `mcp.isBatch` (préfixe de clé configurable) ;
- log applicatif structuré, niveau `INFO` pour `tools/call` (méthode + outil),
  `DEBUG` pour le reste (payload tronqué à 4096 caractères, comme les filtres existants) ;
- point d'extension : interface `McpEventListener` (callback
  `onRequest(McpRequestInfo)`, `onResponseChunk(...)`, `onComplete(...)`) injectable en
  bean pour découpler l'analyse (audit, métriques Micrometer) du filtre lui-même.

## 4. Exigences non fonctionnelles

- **ENF-1 — Non-blocage** : aucun appel bloquant dans le pipeline (contrainte WebFlux).
  Tout est exprimé en `Mono`/`Flux`.
- **ENF-2 — Empreinte mémoire bornée** : au plus `max-request-bytes` par requête en vol ;
  zéro rétention côté réponse (hors fenêtre SSE optionnelle bornée).
- **ENF-3 — Transparence** : octets proxifiés strictement identiques à l'original,
  requête comme réponse (le filtre est invisible pour le client et le serveur MCP).
  Seule exception : le rejet `413` des requêtes dépassant `max-request-bytes` (EF-2).
- **ENF-4 — Ordre du filtre** : `GlobalFilter` ordonné
  `NettyWriteResponseFilter.WRITE_RESPONSE_FILTER_ORDER - 1` (même position que
  `LoggingGlobalFilter`) afin de décorer la réponse avant son écriture Netty, et après
  la résolution de route (l'attribut route est nécessaire pour EF-1).
- **ENF-5 — Configuration** sous le préfixe `mcp.listener.*` dans `application.yaml` :
  `enabled` (défaut `true`), `route-ids`, `path-patterns`, `max-request-bytes`,
  `max-sse-event-bytes`, `log-payloads` (défaut `true`).
- **ENF-6 — Sécurité des logs** : payloads tronqués ; pas de log des headers
  d'authentification (`Authorization`, etc.).

## 5. Conception proposée

```
McpListenerFilter (GlobalFilter, Ordered)
 ├─ matches(exchange)            ← EF-1 (route id / path, config)
 ├─ branche requête
 │    DataBufferUtils.join(body, maxRequestBytes)
 │      → parse JSON-RPC (Jackson) → McpRequestInfo     ← EF-2, EF-3, EF-4
 │      → exchange.attributes + listeners               ← EF-7
 │      → ServerHttpRequestDecorator (ré-émission des octets)
 └─ branche réponse
      ServerHttpResponseDecorator
        ├─ writeWith(...)          doOnNext(observe)    ← EF-5, EF-6
        └─ writeAndFlushWith(...)  map(inner → doOnNext(observe))
             observe = SseEventAssembler (framing, état par flux)
                       → NonBlockingJsonParser Jackson 3 (parsing incrémental)
```

- Parsing requête : Jackson (`tools.jackson.databind.ObjectMapper`, déjà présent via
  Spring Boot 4) ; instance partagée, thread-safe.
- Parsing réponse SSE : `tools.jackson.core.JsonFactory
  .createNonBlockingByteArrayParser(ObjectReadContext)` — un parser par évènement SSE
  en cours, alimenté chunk par chunk via `nonBlockingInputFeeder()`.
- `McpRequestInfo` : record immuable porté par les attributs d'exchange.
- L'asymétrie est le cœur de la conception : **`join` côté requête** (payload petit,
  fini, nécessaire en entier pour le parsing JSON, rejet `413` au-delà de la limite)
  vs **`doOnNext` + parsing incrémental côté réponse** (flux potentiellement infini,
  jamais agrégé, jamais interrompu).

## 6. Cas de test (JUnit 5 + `StepVerifier` / `WireMock`)

1. `tools/call` → attributs `mcp.method` / `mcp.toolName` renseignés, backend reçoit le
   payload octet-pour-octet identique.
2. Notification sans `id`, message batch, `initialize` → extraction conforme EF-3.
3. JSON invalide → pass-through intact, `WARN` logué, pas d'erreur HTTP.
4. Corps > `max-request-bytes` → réponse `413` avec corps d'erreur JSON-RPC, requête
   non transmise au backend, buffers libérés.
5. Réponse SSE longue → chaque évènement reçu par le client dès son émission par le
   backend (vérification du non-buffering : timestamps inter-évènements préservés),
   mémoire stable.
5bis. Évènement SSE coupé à cheval sur plusieurs chunks (coupure au milieu d'une ligne
   `data:` et au milieu du JSON) → frontières correctement détectées, JSON-RPC de
   réponse extrait via le parser non-bloquant, octets proxifiés inchangés ; évènement
   > `max-sse-event-bytes` → abandonné avec `WARN`, flux non perturbé.
6. Réponse `application/json` simple → métriques/logs corrects.
7. Trafic non-MCP (`/ollama/**`) → aucune décoration appliquée.
8. Requête `GET` (ouverture de flux SSE) et `DELETE` (fin de session) → pas de
   bufferisation, `Mcp-Session-Id` capturé.
9. Absence de fuite de `DataBuffer` (compteur de références Netty) sur tous les chemins,
   y compris annulation client en cours de stream.

## 7. Hors périmètre / évolutions envisageables

- Corrélation requête/réponse par `id` JSON-RPC sur la durée d'une session.
- Politique de blocage d'outils (`tools/call` interdits) s'appuyant sur `mcp.toolName`.
- Export OpenTelemetry des spans MCP.
