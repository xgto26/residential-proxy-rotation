# python aiohttp proxy: How to Route Async Scrapers Through Residential IPs (Auth, SOCKS5, Rotation, Retries)

Most people arrive here with a snippet that already worked in `requests`:

python
session.get(url, proxies={"http": "http://user:pass@host:port"})


Run that against aiohttp and you get a `TypeError` instead of a response. That's the small problem. The bigger one shows up two weeks later, when your script is pulling 50,000 URLs through a single IP and every third response is a 403, a CAPTCHA page, or a connection reset.

This is a walkthrough of the parts that actually decide whether an async scraping job survives: how aiohttp wants proxies passed, how auth and SOCKS5 differ, how to rotate without spawning a hundred sessions, and what a proxy plan should cost when your workload is concurrency-heavy rather than request-heavy. The pricing numbers at the end come from 9Proxy's residential network, because the plan model there (per-IP vs per-GB) maps almost exactly onto the two shapes an asyncio pipeline can take.

## The one argument that doesn't exist: `proxies=`

aiohttp has no `proxies` dict. It takes a single `proxy` string, per request:

python
import asyncio
import aiohttp

PROXY = "http://USERNAME:PASSWORD@HOST:PORT"

async def main():
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://api.ipify.org?format=json",
            proxy=PROXY,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            print(await resp.json())

asyncio.run(main())


The URL format is the familiar one:


<scheme>://[<username>:<password>@]<host>[:<port>]


`scheme` is `http` or `https` — that describes the *proxy*, not the target site. You don't need a separate scheme for HTTPS targets; the proxy handles those over `CONNECT`.

Two consequences worth internalising before you write more code:

- There is no per-scheme mapping. If you were relying on `{"http": ..., "https": ...}` to send different traffic to different gateways, you now do that branching yourself, per request.
- `proxy` is evaluated per call, which is exactly what you want for rotation. One `ClientSession` can talk to twenty different proxies on twenty consecutive requests, because aiohttp keys pooled connections by proxy as well as by host and port. You are not forced into a session-per-proxy design unless you choose SOCKS5.

## Authentication: inline credentials vs `BasicAuth`

Both of these work:

python
# 1. Credentials inside the URL
await session.get(url, proxy="http://user:pass@host:port")

# 2. Credentials passed separately
auth = aiohttp.BasicAuth("user", "pass")
await session.get(url, proxy="http://host:port", proxy_auth=auth)


The second form is nicer when credentials come from environment variables, since you don't have to escape special characters in the password before string-interpolating them into a URL. A `@` or `#` inside an unescaped password will silently produce a broken URL, and the error you get back is usually a confusing 407 rather than anything mentioning parsing.

While you're at env vars, watch `trust_env`. A session created with `trust_env=True` picks up `HTTP_PROXY` / `HTTPS_PROXY` from the environment, and combining that with an explicit `proxy` argument is a reliable way to get an exception or traffic routed somewhere you didn't intend. For a rotating pipeline, build the session with `trust_env=False` and pass every proxy explicitly.

## SOCKS5: useful, but it changes your architecture

aiohttp only speaks HTTP proxies natively. For SOCKS4/SOCKS5 you need the `aiohttp-socks` package, which supplies a connector built on `python-socks`:

python
import aiohttp
from aiohttp_socks import ProxyConnector

connector = ProxyConnector.from_url("socks5://user:pass@host:port")

async with aiohttp.ClientSession(connector=connector) as session:
    async with session.get("https://example.com") as resp:
        print(await resp.text())


The catch: a `ProxyConnector` is bound to the session it was created with. One connector, one proxy. So a SOCKS5 rotation setup means creating and closing a session per proxy — fine for fifty proxies you reuse all day, wasteful for thousands of one-shot endpoints from a rotating pool. It also costs you connection pooling benefits, since each new proxy gets a cold pool.

For plain HTTP scraping, HTTP proxy URLs are the simpler choice: rotate per request, keep one warm session, keep the pool. SOCKS5 earns its place when you're tunnelling something that isn't HTTP, or when a tool further down the stack insists on it. 9Proxy publishes both HTTP/HTTPS and SOCKS5 endpoint formats, so you can test either against the same credentials.

## Concurrency, timeouts and retries: where success rates actually come from

The single most common mistake in an async proxy script is unbounded concurrency. `asyncio.gather` on 10,000 URLs will open 10,000 sockets, exhaust file descriptors, and get you throttled by the proxy gateway before the target site even sees you.

A structure that holds up:

python
import asyncio
import random
import aiohttp

TIMEOUT = aiohttp.ClientTimeout(total=30, sock_connect=10, sock_read=15)
RETRY_STATUS = {403, 407, 429, 500, 502, 503, 504}

async def fetch(session, url, get_proxy, sem, attempts=3):
    async with sem:
        for attempt in range(1, attempts + 1):
            proxy = get_proxy()          # fresh endpoint per attempt
            try:
                async with session.get(url, proxy=proxy, timeout=TIMEOUT) as resp:
                    if resp.status in RETRY_STATUS:
                        raise aiohttp.ClientResponseError(
                            resp.request_info, resp.history, status=resp.status
                        )
                    return await resp.text()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == attempts:
                    return None
                await asyncio.sleep(1.5 ** attempt + random.random())

async def main():
    connector = aiohttp.TCPConnector(limit=200, ttl_dns_cache=300)
    sem = asyncio.Semaphore(50)
    async with aiohttp.ClientSession(connector=connector, trust_env=False) as session:
        ...


Four details in there matter more than the rest:

- **Split the timeout.** `sock_connect` and `sock_read` separate "the proxy never answered" from "the target is slow". A 30-second total with no read timeout means one dead proxy holds a slot for half a minute.
- **Retry on the right statuses.** 403 and 429 usually mean the IP is burned — a new endpoint fixes it. 407 means your auth is wrong, and retrying twenty times won't help.
- **Rotate on retry, not just on schedule.** Swapping the proxy inside the retry loop is what turns a 70% success rate into something usable.
- **Cap with a semaphore, not with `limit_per_host`.** Behind a rotating pool, per-host limits are close to meaningless; the semaphore is what protects the proxy gateway.

One more: on `ssl`. You'll find tutorials that set `ssl=False` on every proxy request. With an ordinary CONNECT proxy, TLS goes end-to-end between your client and the target, so certificate verification still works and you should leave it on. Disabling it hands anyone on the path — including a misconfigured middlebox — the ability to read and alter your traffic, and it hides real failures behind a green checkmark.

## Sticky or rotating: both belong in the same script

Rotating is one decision, not a global setting. Two very different jobs:

- **Rotating per request** for breadth — SERP checks, price sampling, ad verification across regions. Each request is disposable.
- **Sticky for minutes or hours** for anything with state — a session that logs in, fills a cart, or reads paginated results. Rotate mid-flow and you look like a different person halfway through a conversation.

Good providers expose both as a switch on the same endpoint rather than as separate products. 9Proxy's GB-based proxies offer rotating and sticky modes with targeting down to country, state, city, ZIP and ISP, all driven from the dashboard generator — which is the setting you'll be toggling most when your aiohttp script needs a stable IP for one flow and fresh IPs for everything else.

## What the plan model means for an async pipeline

This is where most guides stop, and it's the part that decides your architecture.

Residential proxies are sold two ways. Per IP with unlimited bandwidth, or per GB of traffic. Under `asyncio`, the two map onto different scripts:

|  | Per-IP model | Per-GB model |
| --- | --- | --- |
| Billed by | Number of IPs | Traffic consumed |
| Bandwidth | Unlimited while the IP is active | Capped by purchased GB |
| Endpoints | Managed per IP | Generate unlimited endpoints |
| Best fit in asyncio | Long sessions, heavy downloads, fixed-identity flows | High-rotation breadth, small responses |
| Validity | Unused IPs don't expire; active IPs live a few hours up to ~24h | 180 days (unlimited on Enterprise) |
| Auth | Desktop app with local port forwarding, optional proxy auth | Username/password or IP whitelist, no app |

That last row is the one that catches people out, so it's worth stating plainly. **9Proxy's per-IP product runs through a desktop app that forwards traffic to a local port** — you point aiohttp at something like `127.0.0.1:<port>` and the app maps it to a residential IP. Perfect on your own Windows machine. Useless on a headless cloud VM or inside a container, where there's no desktop to run.

The GB-based product is the opposite: endpoints are generated in the dashboard, authenticated with a username and password (or by whitelisting your outbound IP), and there's no app involved. For a cron-driven scraper on a VPS, that's the only one of the two that works without a workaround.

If you're doing browser automation on your own machine with a handful of long-lived profiles, per-IP with unlimited bandwidth is the better deal — the cost stops caring how much data you move. If you're running an async pipeline that fires tens of thousands of small JSON or HTML requests, per-GB is cheaper, because you're paying for bytes you actually use instead of burning an IP per request.

### Full pricing at a glance

Prices below reflect the rates after 9Proxy's June 1, 2026 adjustment, which raised IP-based and bundle pricing and left GB-based packages untouched.

**IP-based residential packages (unlimited bandwidth per activated IP)**

| Package | Price | Effective per IP | Buy |
| --- | --- | --- | --- |
| 100 IPs | $24 | $0.24 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 500 IPs | $72 | $0.144 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 1,000 + 500 bonus IPs | $126 | $0.084 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 2,500 IPs | $210 | $0.084 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 5,000 IPs | $360 | $0.072 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 15,000 IPs | $720 | $0.048 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 25,000 IPs | $863 | $0.035 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 50,000 IPs | $1,438 | $0.029 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 100,000 IPs (Business) | $2,300 | $0.023 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 200,000 IPs (Business) | $4,140 | $0.021 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 500,000 IPs (Business) | $8,625 | $0.018 | [ 购买/注册](https://bit.ly/9-Proxy) |

**GB-based residential packages (180-day validity; unlimited on Enterprise)**

| Package | Price | Effective per GB | Buy |
| --- | --- | --- | --- |
| 5 GB | $15 | $3.00 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 50 GB + 5 GB bonus | $105 | $2.10 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 100 GB | $150 | $1.50 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 200 GB | $200 | $1.00 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 1,000 GB | $800 | $0.80 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 2,000 GB | $1,500 | $0.75 | [ 购买/注册](https://bit.ly/9-Proxy) |
| 10,000 GB | $6,800 | $0.68 | [ 购买/注册](https://bit.ly/9-Proxy) |

**Bundle packages (IPs + traffic in one purchase)**

| Package | Contents | Price | Buy |
| --- | --- | --- | --- |
| Starter Bundle | 100 IPs + 5 GB | $30 | [ 购买/注册](https://bit.ly/9-Proxy) |
| Popular Bundle | 1,500 IPs + 50 GB | $180 | [ 购买/注册](https://bit.ly/9-Proxy) |
| Pro Bundle | 5,000 IPs + 500 GB | $720 | [ 购买/注册](https://bit.ly/9-Proxy) |
| Enterprise | Custom volume, unlimited data validity, 1 owner + up to 5 team members | Custom pricing | [ 购买/注册](https://bit.ly/9-Proxy) |

> Prices change and bundles have been renamed between third-party write-ups (the mid-tier appears as both "Popular" and "Growth"), so treat the checkout total as the number that counts. Datacenter proxies are listed as coming soon and aren't purchasable yet.

## Running the numbers against your own script

Before picking a tier, estimate the request size. A typical HTML page or JSON response runs around 40–60 KB, which puts 5 GB at roughly 90,000–120,000 requests. If you're scraping product pages, that 5 GB pack is a couple of weeks of moderate work for $15 — and $0.68/GB at the high tier beats it by more than 4x per request.

The per-IP model flips the logic. 500 IPs for $72 has no bandwidth ceiling, so a job that downloads images, PDFs, or full-page screenshots at 2 MB apiece costs the same as one that pulls 40 KB JSON. Two other per-IP features matter for long sessions: unused IP balance doesn't expire until you activate it, and the Today List lets you reuse an IP you already forwarded within 24 hours for free, as long as it's still online. If your pipeline keeps a fixed set of identities alive, that reuse habit cuts effective cost noticeably.

On promos: 9Proxy runs them on a schedule rather than keeping a permanent coupon field. An April 2026 campaign auto-issued a 9% coupon to anyone who placed a GB order that month, valid on GB orders only and expiring June 30, 2026. Timing a purchase around a live campaign is the reliable way to pay less. Signing up through a referral link is the other one — the referred account gets 5% off as part of the affiliate program.

## Failure modes worth memorising

If your first proxy run through aiohttp misbehaves, it's almost always one of these:

1. **407 Proxy Authentication Required** — credentials wrong, or your egress IP isn't on the whitelist while you're also sending username/password. Pick one auth method.
2. **403 or 429 on every attempt** — the IP pool isn't the problem, your request pattern is. Add a delay, rotate the User-Agent, and stop hammering the same target at 200 requests per second.
3. **`ServerDisconnectedError` in bursts** — concurrency set too high for the gateway. Drop the semaphore and raise it slowly.
4. **SSL errors that vanish with `ssl=False`** — usually a MITM or a bad proxy cert, not something to paper over in production.
5. **Memory climbing steadily** — response bodies not being read or released. Always consume or release inside the `async with`.
6. **A SOCKS5 connector reused across proxies** — it can't be. One connector, one proxy.

## Quick answers

**Does aiohttp support proxies out of the box?** Yes for HTTP/HTTPS proxies via the per-request `proxy` argument and `proxy_auth`. SOCKS needs `aiohttp-socks`.

**Can one `ClientSession` use multiple proxies?** Yes. Pass a different `proxy` per request — the connection pool keys on the proxy, so endpoints stay isolated.

**Do I need the desktop app?** Only for IP-based packages. GB-based endpoints are generated in the dashboard and work from any headless environment.

**What's the cheapest way to test an async setup?** $15 for 5 GB, or $24 for 100 IPs with unlimited bandwidth. Trial access exists but depends on availability, so ask support rather than assuming.

**Is unlimited bandwidth real?** On the per-IP product, yes — traffic isn't metered while an IP is active. On the per-GB product you're buying a fixed traffic pool, so bandwidth is the thing you're paying for.

## Where to start

Get the aiohttp layer working against a single endpoint first — one session, one proxy, one `await session.get()`. Confirm the returned IP is the one you expect, not your own. Then add the semaphore, the split timeouts, and rotation-on-retry, and only after that turn the concurrency up.

If you'd rather solve the endpoint side in one step, [👉 注册 9Proxy 并领取推荐折扣](https://bit.ly/9-Proxy) gets you dashboard-generated credentials plus ready-made code samples in the proxy generator, so the jump from your first request to a rotating pool is mostly configuration rather than plumbing. Start with the smallest pack that covers a week of real traffic, measure your actual GB per thousand requests, and size the second purchase from that number instead of from a guess.
