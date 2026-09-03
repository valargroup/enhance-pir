use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{extract::State, response::Html};
use tokio::sync::RwLock;

use crate::{
    alerts::Alert,
    host::HostHealth,
    metrics::{EndpointWindow, LatencyWindow},
    schema::Schema,
    thresholds,
};

pub type SharedDashboard = Arc<RwLock<DashboardData>>;

/// Seconds between browser refreshes. Matched to the default scrape interval so
/// the page turns over at roughly the same rate the data behind it does.
const REFRESH_SECONDS: u64 = 15;

#[derive(Clone, Debug)]
pub struct DashboardData {
    pub title: String,
    pub schema: Schema,
    pub environment: String,
    pub hostname: String,
    pub last_scrape: Option<SystemTime>,
    pub scrape_error: Option<String>,
    pub health_status: Option<u16>,
    pub health_body: Option<String>,
    pub ready_status: Option<u16>,
    pub endpoints: BTreeMap<String, EndpointWindow>,
    pub snapshot_gauges: BTreeMap<String, f64>,
    pub workers: BTreeMap<String, BTreeMap<String, f64>>,
    pub layout: BTreeMap<String, f64>,
    pub tables: BTreeMap<String, BTreeMap<String, f64>>,
    pub worker_tables: BTreeMap<String, BTreeMap<String, BTreeMap<String, f64>>>,
    pub process_resident_memory_bytes: Option<f64>,
    pub host: HostHealth,
    pub active_alerts: Vec<Alert>,
    pub recent_alerts: Vec<(SystemTime, String)>,
}

impl DashboardData {
    pub fn new(
        title: String,
        schema: Schema,
        environment: String,
        hostname: String,
        host: HostHealth,
    ) -> Self {
        Self {
            title,
            schema,
            environment,
            hostname,
            last_scrape: None,
            scrape_error: Some("waiting for first scrape".to_string()),
            health_status: None,
            health_body: None,
            ready_status: None,
            endpoints: BTreeMap::new(),
            snapshot_gauges: BTreeMap::new(),
            workers: BTreeMap::new(),
            layout: BTreeMap::new(),
            tables: BTreeMap::new(),
            worker_tables: BTreeMap::new(),
            process_resident_memory_bytes: None,
            host,
            active_alerts: Vec::new(),
            recent_alerts: Vec::new(),
        }
    }
}

pub async fn index(State(state): State<SharedDashboard>) -> Html<String> {
    let data = state.read().await.clone();
    Html(render(&data))
}

pub async fn healthz() -> &'static str {
    "ok\n"
}

const STYLE: &str = r#"
*,*::before,*::after{box-sizing:border-box}
:root{
color-scheme:dark;
--vellum:#17150f;--ink:#efe5d0;--gold:#c6a15b;--lapis:#3a95dc;
--parchment:188,169,138;
--p85:rgba(var(--parchment),.85);--p62:rgba(var(--parchment),.62);
--p42:rgba(var(--parchment),.42);--p22:rgba(var(--parchment),.22);
--p16:rgba(var(--parchment),.16);--p08:rgba(var(--parchment),.08);
--p05:rgba(var(--parchment),.05);--p02:rgba(var(--parchment),.02);
--ok:#8fb573;--warn:#c6a15b;--bad:#d2694f;
--sans:"IBM Plex Sans",ui-sans-serif,system-ui,-apple-system,sans-serif;
--mono:"IBM Plex Mono",ui-monospace,SFMono-Regular,Menlo,monospace;
}
html{-webkit-text-size-adjust:100%}
body{margin:0;background:var(--vellum);color:var(--p85);font-family:var(--sans);
font-size:15px;line-height:1.55;letter-spacing:-.005em;-webkit-font-smoothing:antialiased}
#app{max-width:1180px;margin:0 auto;padding:clamp(28px,5vh,56px) clamp(16px,4vw,32px) 72px}
h1,h2,h3{color:var(--ink);font-weight:400;margin:0}
code{font-family:var(--mono);color:var(--p62);font-size:.92em}
.num{font-family:var(--mono);font-variant-numeric:tabular-nums;font-feature-settings:"tnum" 1}
.ok{color:var(--ok)}.warn{color:var(--warn)}.bad{color:var(--bad)}.muted{color:var(--p42)}
.eyebrow{font-size:10.5px;letter-spacing:.13em;text-transform:uppercase;color:var(--p42);
font-weight:500;margin:0 0 16px}

.masthead{display:flex;flex-wrap:wrap;gap:20px;align-items:flex-end;
justify-content:space-between;padding-bottom:22px;border-bottom:1px solid var(--p16)}
.brand{display:flex;align-items:center;gap:14px}
.mark{width:26px;height:26px;flex:none;border:1px solid var(--gold);
transform:rotate(45deg);position:relative}
.mark::after{content:"";position:absolute;inset:5px;background:var(--gold);opacity:.55}
.brand h1{font-size:22px;letter-spacing:.055em;text-transform:uppercase;line-height:1.1}
.brand .sub{margin:2px 0 0;font-size:11px;letter-spacing:.1em;text-transform:uppercase;
color:var(--p42)}
.tags{display:flex;flex-wrap:wrap;gap:8px}
.pill{border:1px solid var(--p22);border-radius:999px;padding:4px 12px;font-size:11px;
letter-spacing:.08em;text-transform:uppercase;color:var(--p62);white-space:nowrap}
.pill.env{border-color:rgba(198,161,91,.45);color:var(--gold)}

.statusbar{display:flex;flex-wrap:wrap;gap:12px 24px;align-items:center;
justify-content:space-between;padding:16px 0 30px;border-bottom:1px solid var(--p08)}
.chips{display:flex;flex-wrap:wrap;gap:8px}
.chip{display:inline-flex;align-items:center;gap:7px;border:1px solid var(--p16);
border-radius:4px;padding:5px 11px;font-size:11.5px;letter-spacing:.06em;
text-transform:uppercase;color:var(--p62);background:var(--p02)}
.chip .dot{width:6px;height:6px;border-radius:50%;flex:none;background:currentColor}
.chip.is-ok{color:var(--ok);border-color:rgba(143,181,115,.3)}
.chip.is-bad{color:var(--bad);border-color:rgba(210,105,79,.35)}
.chip b{font-weight:500;font-family:var(--mono)}
.updated{display:flex;align-items:center;gap:9px;font-size:12px;color:var(--p42)}
.live{width:6px;height:6px;border-radius:50%;background:var(--ok);flex:none;
animation:pulse 2.4s ease-in-out infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}

.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:var(--p16);
border:1px solid var(--p16);margin:30px 0 34px}
.kpi{background:var(--vellum);padding:20px 22px 22px}
.kpi .figure{font-family:var(--mono);font-variant-numeric:tabular-nums;font-size:30px;
line-height:1.1;color:var(--ink);margin:0;letter-spacing:-.02em}
.kpi .figure.warn{color:var(--warn)}.kpi .figure.bad{color:var(--bad)}
.kpi .unit{margin:7px 0 0;font-size:11.5px;color:var(--p42);letter-spacing:.02em}
.kpi .eyebrow{margin-bottom:12px}

.card{border:1px solid var(--p16);background:var(--p02);padding:22px 24px 24px;
margin-bottom:20px;min-width:0;overflow:hidden}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:20px}
.grid .card{margin:0}
.card .rows+.note,.card .meter+.rows{margin-top:22px}

table{width:100%;border-collapse:collapse;font-size:13.5px}
thead th{font-size:10.5px;letter-spacing:.11em;text-transform:uppercase;color:var(--p42);
font-weight:500;text-align:right;padding:0 0 12px 16px;border-bottom:1px solid var(--p16);
white-space:nowrap}
thead th:first-child{text-align:left;padding:0 16px 12px 0}
tbody th,tbody td{padding:13px 0;border-bottom:1px solid var(--p08);text-align:right;
font-family:var(--mono);font-variant-numeric:tabular-nums;white-space:nowrap}
tbody th{font-weight:400;text-align:left;color:var(--ink);font-size:13px;padding-right:16px}
tbody td{padding-left:16px}
tbody tr:last-child th,tbody tr:last-child td{border-bottom:0}
.wrap{overflow-x:auto;margin:0 -4px;padding:0 4px}
.errcell{display:flex;flex-direction:column;align-items:flex-end;gap:5px}
.errbar{width:52px;height:2px;background:var(--p08)}
.errbar i{display:block;height:100%;background:var(--bad)}

.rows{list-style:none;margin:0;padding:0}
.rows li{display:flex;justify-content:space-between;gap:18px;padding:11px 0;
border-bottom:1px solid var(--p08);font-size:13.5px}
.rows li:last-child{border-bottom:0}
.rows .k{color:var(--p62)}
.rows .v{font-family:var(--mono);font-variant-numeric:tabular-nums;color:var(--ink);
text-align:right;white-space:nowrap}
.rows .v.wrap-any{white-space:normal;overflow-wrap:anywhere;word-break:break-all;min-width:0}

.meter{margin-top:22px}
.meter:first-of-type{margin-top:0}
.meter-head{display:flex;justify-content:space-between;gap:16px;font-size:12.5px;
align-items:baseline;margin-bottom:9px}
.meter-head .k{color:var(--p62)}
.meter-head .v{font-family:var(--mono);font-variant-numeric:tabular-nums;color:var(--p85);
white-space:nowrap}
.meter-head .v.warn{color:var(--warn)}
.meter-head .v.bad{color:var(--bad)}
.meter-foot{margin:9px 0 0;font-size:11.5px;color:var(--p42);font-family:var(--mono);
font-variant-numeric:tabular-nums}
.bar{height:3px;background:var(--p08);overflow:hidden}
.bar i{display:block;height:100%;background:var(--p42)}
.bar i.warn{background:var(--warn)}
.bar i.bad{background:var(--bad)}

.alerts{list-style:none;margin:0;padding:0}
.alerts li{border-left:2px solid var(--p22);padding:2px 0 2px 14px;margin-bottom:16px;
font-size:13.5px}
.alerts li:last-child{margin-bottom:0}
.alerts li.is-bad{border-left-color:var(--bad)}
.alerts li.is-ok{border-left-color:var(--ok)}
.alerts .name{color:var(--ink);font-family:var(--mono);font-size:13px}
.alerts .detail{margin:3px 0 0;color:var(--p62)}
.alerts .when{margin:3px 0 0;font-size:11.5px;color:var(--p42);font-family:var(--mono)}
.empty{color:var(--p42);font-size:13.5px;margin:0}

.note{border:1px solid var(--p08);background:transparent;padding:20px 24px;
font-size:12.5px;color:var(--p42);line-height:1.7;margin:0}
.note strong{color:var(--p62);font-weight:500}
.note code{display:block;margin-top:8px;white-space:pre-wrap;overflow-wrap:anywhere;
word-break:break-all;line-height:1.5}

.topo{display:flex;flex-direction:column;align-items:center;gap:0;margin:6px 0 22px}
.node{border:1px solid var(--p22);background:var(--p02);padding:12px 18px;min-width:220px;
max-width:340px;text-align:center}
.node .desc{font-size:11.5px;color:var(--p62);margin:7px 0 0;line-height:1.45}
.node .meta.warn{color:var(--warn)}.node .meta.bad{color:var(--bad)}
.tables{width:100%;display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));
gap:12px;margin-top:26px}
.tables .node{min-width:0;max-width:none;text-align:left;padding:12px 14px}
.tables .node .meter{margin-top:10px}
.tables .node .meter-foot{margin-top:6px}
.node.planned{opacity:.55;border-style:dashed}
.node .kv{font-family:var(--mono);font-size:11.5px;color:var(--p62);margin:4px 0 0;
overflow-wrap:anywhere}
.node .kv b{color:var(--p85);font-weight:500}
.topo .section{width:100%;margin-top:24px;font-size:10.5px;letter-spacing:.13em;
text-transform:uppercase;color:var(--p42);text-align:left}
.explain{margin:0;display:grid;grid-template-columns:minmax(120px,180px) 1fr;gap:14px 22px;
font-size:13.5px}
.explain dt{color:var(--ink);font-family:var(--mono);font-size:12.5px;padding-top:2px}
.explain dd{margin:0;color:var(--p62);line-height:1.6}
.explain dd b{color:var(--p85);font-weight:500;font-family:var(--mono);font-size:.95em}
@media (max-width:720px){.explain{grid-template-columns:1fr;gap:6px 0}.explain dd{margin-bottom:10px}}
.node .role{font-size:10.5px;letter-spacing:.13em;text-transform:uppercase;color:var(--p42);
margin:0 0 4px}
.node .id{font-family:var(--mono);font-size:13px;color:var(--ink);margin:0;
overflow-wrap:anywhere}
.node .meta{font-family:var(--mono);font-size:11.5px;color:var(--p62);margin:5px 0 0;
overflow-wrap:anywhere}
.node.coord{border-color:rgba(198,161,91,.55)}
.node.is-ok{border-color:rgba(143,181,115,.5)}
.node.is-bad{border-color:rgba(210,105,79,.6)}
.node.is-warn{border-color:rgba(198,161,91,.6)}
.trunk{width:1px;height:22px;background:var(--p22)}
.bus{position:relative;width:100%;display:flex;justify-content:center;gap:16px;
flex-wrap:wrap;padding-top:22px}
.bus::before{content:"";position:absolute;left:10%;right:10%;top:0;height:1px;
background:var(--p22)}
.bus .leaf{position:relative}
.bus .leaf::before{content:"";position:absolute;left:50%;top:-22px;width:1px;height:22px;
background:var(--p22)}
.node .state{display:inline-block;margin-top:7px;font-size:10.5px;letter-spacing:.1em;
text-transform:uppercase}
.node.is-ok .state{color:var(--ok)}.node.is-bad .state{color:var(--bad)}
.node.is-warn .state{color:var(--warn)}
@media (max-width:900px){.kpis{grid-template-columns:repeat(2,1fr)}}
@media (max-width:720px){
#app{padding-bottom:48px}
.kpis{grid-template-columns:1fr}
.grid{grid-template-columns:1fr}
.masthead,.statusbar{align-items:flex-start;flex-direction:column}
.kpi .figure{font-size:26px}
}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
"#;

const SCRIPT: &str = r#"
(function () {
  var PERIOD = 15000;
  var timer;
  function schedule() { clearTimeout(timer); timer = setTimeout(run, PERIOD); }
  function run() {
    if (document.hidden) { schedule(); return; }
    fetch(location.href, { cache: 'no-store', credentials: 'same-origin' })
      .then(function (response) { return response.ok ? response.text() : null; })
      .then(function (text) {
        if (!text) return;
        var next = new DOMParser().parseFromString(text, 'text/html').getElementById('app');
        var current = document.getElementById('app');
        if (next && current) current.replaceWith(next);
      })
      .catch(function () {})
      .then(schedule);
  }
  document.addEventListener('visibilitychange', function () {
    if (!document.hidden) { clearTimeout(timer); run(); }
  });
  schedule();
})();
"#;

fn render(data: &DashboardData) -> String {
    let mut out = String::with_capacity(16 * 1024);
    out.push_str("<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">");
    out.push_str("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
    out.push_str(&format!("<title>{}</title>", escape(&data.title)));
    out.push_str(&format!(
        "<noscript><meta http-equiv=\"refresh\" content=\"{REFRESH_SECONDS}\"></noscript>"
    ));
    out.push_str("<style>");
    out.push_str(STYLE);
    out.push_str("</style></head><body><main id=\"app\">");

    out.push_str(&masthead(data));
    out.push_str(&statusbar(data));
    out.push_str(&kpis(data));
    out.push_str(&endpoint_table(data));
    out.push_str(&processing_latency_splits(data));
    out.push_str(&fleet_card(data));
    out.push_str(&explainer_card(data));
    out.push_str("<div class=\"grid\">");
    out.push_str(&service_card(data));
    out.push_str(&host_card(&data.host));
    out.push_str(&active_alerts_card(&data.active_alerts));
    out.push_str(&recent_alerts_card(&data.recent_alerts));
    out.push_str("</div>");
    out.push_str(&privacy_note());

    out.push_str("</main><script>");
    out.push_str(SCRIPT);
    out.push_str("</script></body></html>");
    out
}

fn masthead(data: &DashboardData) -> String {
    format!(
        "<header class=\"masthead\">\
<div class=\"brand\"><span class=\"mark\"></span>\
<div><h1>{title}</h1><p class=\"sub\">Valar Group</p></div></div>\
<div class=\"tags\"><span class=\"pill env\">{environment}</span>\
<span class=\"pill\">{hostname}</span></div></header>",
        title = escape(&data.title),
        environment = escape(&data.environment),
        hostname = escape(&data.hostname),
    )
}

fn statusbar(data: &DashboardData) -> String {
    let scrape = match &data.scrape_error {
        Some(error) => chip("scrape", &escape(error), false),
        None => chip("scrape", "ok", true),
    };
    let updated = match data.last_scrape {
        Some(at) => format!(
            "<span class=\"live\"></span>Updated {} &middot; {}",
            relative_time(at),
            time(at)
        ),
        None => "<span class=\"live\"></span>Awaiting first scrape".to_string(),
    };
    format!(
        "<div class=\"statusbar\"><div class=\"chips\">{scrape}{health}{ready}</div>\
<div class=\"updated\">{updated}</div></div>",
        health = status_chip("health", data.health_status),
        ready = status_chip("ready", data.ready_status),
    )
}

fn chip(label: &str, value: &str, ok: bool) -> String {
    let tone = if ok { "is-ok" } else { "is-bad" };
    format!("<span class=\"chip {tone}\"><span class=\"dot\"></span>{label} <b>{value}</b></span>")
}

fn status_chip(label: &str, code: Option<u16>) -> String {
    match code {
        Some(code) => chip(label, &code.to_string(), (200..300).contains(&code)),
        None => chip(label, "n/a", false),
    }
}

/// Endpoints in display order: configured first, then anything the server
/// labelled that the config did not list.
fn ordered_endpoints(data: &DashboardData) -> Vec<(&str, EndpointWindow)> {
    let mut out: Vec<(&str, EndpointWindow)> = data
        .schema
        .endpoints
        .iter()
        .map(|endpoint| {
            (
                endpoint.as_str(),
                data.endpoints.get(endpoint).cloned().unwrap_or_default(),
            )
        })
        .collect();
    out.extend(
        data.endpoints
            .iter()
            .filter(|(name, _)| !data.schema.knows(name))
            .map(|(name, window)| (name.as_str(), window.clone())),
    );
    out
}

fn uses_processing(data: &DashboardData, endpoint: &str, window: &EndpointWindow) -> bool {
    data.schema.uses_processing(endpoint) || window.processing_available
}

fn kpis(data: &DashboardData) -> String {
    let ordered = ordered_endpoints(data);
    let windows: Vec<&EndpointWindow> = ordered.iter().map(|(_, window)| window).collect();
    let total_qps: f64 = windows.iter().map(|window| window.qps).sum();
    let worst_p95 = ordered
        .iter()
        .filter(|(endpoint, _)| !data.schema.is_informational(endpoint))
        .filter_map(|(endpoint, window)| {
            window
                .alert_latency(uses_processing(data, endpoint, window))
                .p95
        })
        .fold(None::<f64>, |acc, value| {
            Some(acc.map_or(value, |current: f64| current.max(value)))
        });
    // Round before deciding the tone so a value that displays as "0" is never
    // painted as an error.
    let errors = windows
        .iter()
        .map(|window| window.errors_5xx)
        .sum::<f64>()
        .round();
    let alerts = data.active_alerts.len();

    let mut out = String::from("<section class=\"kpis\">");
    out.push_str(&kpi(
        "Throughput",
        &format!("{total_qps:.3}"),
        "req/s over 5m",
        "",
    ));
    out.push_str(&kpi(
        "Worst server p95",
        &worst_p95
            .map(|value| format!("{value:.3}s"))
            .unwrap_or_else(|| "—".to_string()),
        "slowest alert basis",
        "",
    ));
    out.push_str(&kpi(
        "5xx responses",
        &format!("{errors:.0}"),
        "across all endpoints",
        if errors > 0.0 { "bad" } else { "" },
    ));
    out.push_str(&kpi(
        "Active alerts",
        &alerts.to_string(),
        if alerts == 1 {
            "check firing"
        } else {
            "checks firing"
        },
        if alerts > 0 { "bad" } else { "" },
    ));
    out.push_str("</section>");
    out
}

fn kpi(label: &str, figure: &str, unit: &str, tone: &str) -> String {
    format!(
        "<div class=\"kpi\"><p class=\"eyebrow\">{label}</p>\
<p class=\"figure{tone}\">{figure}</p><p class=\"unit\">{unit}</p></div>",
        tone = class_suffix(tone),
    )
}

/// Renders a tone as a trailing class name, collapsing the empty case so the
/// markup never carries a dangling `class="figure "`.
fn class_suffix(tone: &str) -> String {
    if tone.is_empty() {
        String::new()
    } else {
        format!(" {tone}")
    }
}

fn endpoint_table(data: &DashboardData) -> String {
    let rows = ordered_endpoints(data)
        .into_iter()
        .map(|(endpoint, values)| {
            let budget = if uses_processing(data, endpoint, &values) {
                None
            } else {
                latency_budget(&data.schema, endpoint)
            };
            format!(
                "<tr><th>{name}</th><td>{qps:.3}</td><td>{in_flight:.0}</td>\
<td>{requests:.0}</td>{p50}{p95}{p99}<td>{errors}</td></tr>",
                name = escape(endpoint),
                qps = values.qps,
                in_flight = values.in_flight,
                requests = values.requests,
                p50 = latency_cell(values.observed.p50, budget),
                p95 = latency_cell(values.observed.p95, budget),
                p99 = latency_cell(values.observed.p99, budget),
                errors = error_cell(&values),
            )
        })
        .collect::<String>();
    format!(
        "<section class=\"card\"><p class=\"eyebrow\">Observed PIR request latency &middot; 5 minute window</p>\
<div class=\"wrap\"><table><thead><tr><th>Endpoint</th><th>QPS</th><th>Inflight</th>\
<th>Requests</th><th>p50</th><th>p95</th><th>p99</th><th>5xx</th></tr></thead>\
<tbody>{rows}</tbody></table></div></section>"
    )
}

/// One "latency split" card per endpoint that is paged on processing time.
fn processing_latency_splits(data: &DashboardData) -> String {
    ordered_endpoints(data)
        .into_iter()
        .filter(|(endpoint, window)| uses_processing(data, endpoint, window))
        .map(|(endpoint, _)| processing_latency_split(data, endpoint))
        .collect()
}

fn processing_latency_split(data: &DashboardData, endpoint: &str) -> String {
    let values = data.endpoints.get(endpoint).cloned().unwrap_or_default();
    let threshold = data
        .schema
        .latency_budget(endpoint)
        .unwrap_or(thresholds::DEFAULT_LATENCY_P99_SECONDS);
    let rows = [
        latency_split_row(
            "Observed total",
            &values.observed,
            values.in_flight,
            None,
            "informational",
        ),
        latency_split_row(
            "Server processing",
            &values.processing,
            values.processing_in_flight,
            Some(threshold),
            "pages on p99",
        ),
    ]
    .join("");
    format!(
        "<section class=\"card\"><p class=\"eyebrow\">{name} latency split &middot; 5 minute window</p>\
<div class=\"wrap\"><table><thead><tr><th>Stage</th><th>Samples</th><th>Inflight</th>\
<th>p50</th><th>p95</th><th>p99</th><th>Alerting</th></tr></thead>\
<tbody>{rows}</tbody></table></div>\
<p class=\"note\"><strong>Alert basis.</strong> Server processing begins after the complete request body reaches the coordinator and is the only distribution evaluated against the {threshold:.3}s p99 latency threshold. Body-receive timing is not rendered here.</p></section>",
        name = escape(endpoint),
    )
}

fn latency_split_row(
    label: &str,
    latency: &LatencyWindow,
    in_flight: f64,
    budget: Option<f64>,
    policy: &str,
) -> String {
    format!(
        "<tr><th>{label}</th><td>{samples:.0}</td><td>{in_flight:.0}</td>\
{p50}{p95}{p99}<td class=\"muted\">{policy}</td></tr>",
        label = escape(label),
        samples = latency.samples,
        p50 = latency_cell(latency.p50, budget),
        p95 = latency_cell(latency.p95, budget),
        p99 = latency_cell(latency.p99, budget),
        policy = escape(policy),
    )
}

/// p99 alert threshold for the observed-duration table. Processing endpoints
/// get none because their alert uses the processing-only distribution.
fn latency_budget(schema: &Schema, endpoint: &str) -> Option<f64> {
    schema.observed_budget(endpoint)
}

fn latency_cell(value: Option<f64>, budget: Option<f64>) -> String {
    let Some(seconds) = value else {
        return "<td class=\"muted\">—</td>".to_string();
    };
    let tone = match budget {
        Some(budget) if seconds >= budget => "bad",
        Some(budget) if seconds >= budget * 0.6 => "warn",
        _ => "",
    };
    format!("<td class=\"{tone}\">{seconds:.3}s</td>")
}

fn error_cell(values: &EndpointWindow) -> String {
    let count = values.errors_5xx.round();
    if count <= 0.0 {
        return "<span class=\"muted\">0</span>".to_string();
    }
    let fill = ((values.error_ratio / thresholds::HTTP_5XX_RATIO) * 100.0).clamp(4.0, 100.0);
    format!(
        "<span class=\"errcell\"><span class=\"bad\">{count:.0} ({ratio:.2}%)</span>\
<span class=\"errbar\"><i style=\"width:{fill:.0}%\"></i></span></span>",
        ratio = values.error_ratio * 100.0,
    )
}

/// Enhance is the only active PIR table; unknown labels sort after it.
const TABLE_ORDER: [&str; 1] = ["enhance"];

fn table_rank(name: &str) -> usize {
    TABLE_ORDER
        .iter()
        .position(|known| *known == name)
        .unwrap_or(TABLE_ORDER.len())
}

fn ordered_tables(data: &DashboardData) -> Vec<(&String, &BTreeMap<String, f64>)> {
    let mut tables: Vec<_> = data.tables.iter().collect();
    tables.sort_by(|(a, _), (b, _)| table_rank(a).cmp(&table_rank(b)).then_with(|| a.cmp(b)));
    tables
}

/// `shards a–b` for a pool position, or empty when the inputs are missing.
fn owned_shards(index: Option<f64>, per_worker: Option<f64>) -> String {
    match (index, per_worker) {
        (Some(index), Some(per_worker)) if per_worker >= 1.0 => {
            let first = index * per_worker;
            let last = first + per_worker - 1.0;
            if per_worker == 1.0 {
                format!("shard {}", format_number(first))
            } else {
                format!(
                    "shards {}&ndash;{}",
                    format_number(first),
                    format_number(last)
                )
            }
        }
        _ => String::new(),
    }
}

/// Chain source → coordinator → workers, then one card per PIR table. Only
/// inventory names are shown; worker addresses never reach the page.
fn fleet_card(data: &DashboardData) -> String {
    let gauge = |name: &str| {
        data.snapshot_gauges
            .get(&format!("{}{name}", data.schema.gauge_prefix))
            .copied()
    };
    let phase = gauge("phase_code").map(phase_label).unwrap_or("unknown");
    let generation = gauge("generation").unwrap_or(0.0);
    let coordinator_meta = format!(
        "{phase} &middot; anchor {anchor} &middot; generation {generation}",
        anchor = gauge("anchor_height")
            .map(format_number)
            .unwrap_or_else(|| "—".into()),
        generation = format_number(generation),
    );
    let coordinator_meta2 = [
        gauge("retained_generations")
            .map(|v| format!("{} generations answerable", format_number(v))),
        gauge("ironwood_tree_size").map(|v| format!("{} outputs indexed", format_number(v))),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" &middot; ");

    let mut workers: Vec<(&String, &BTreeMap<String, f64>)> = data.workers.iter().collect();
    workers.sort_by(|(a_name, a), (b_name, b)| {
        let a_index = a.get("index").copied().unwrap_or(f64::INFINITY);
        let b_index = b.get("index").copied().unwrap_or(f64::INFINITY);
        a_index.total_cmp(&b_index).then_with(|| a_name.cmp(b_name))
    });

    let leaves = if workers.is_empty() {
        "<div class=\"leaf\"><div class=\"node\"><p class=\"role\">Workers</p>\
<p class=\"id muted\">none reported yet</p></div></div>"
            .to_string()
    } else {
        workers
            .iter()
            .map(|(name, gauges)| {
                let up = gauges.get("up").copied();
                let worker_generation = gauges.get("generation").copied().unwrap_or(0.0);
                let (tone, state) = match up {
                    Some(value) if value >= 1.0 => {
                        if generation > 0.0
                            && worker_generation > 0.0
                            && worker_generation != generation
                        {
                            ("is-warn", "generation lag")
                        } else {
                            ("is-ok", "healthy")
                        }
                    }
                    Some(_) => ("is-bad", "unreachable"),
                    None => ("", "unknown"),
                };
                let mut shares: Vec<(&String, &BTreeMap<String, f64>)> = data
                    .worker_tables
                    .get(*name)
                    .map(|tables| tables.iter().collect())
                    .unwrap_or_default();
                shares.sort_by(|(a, _), (b, _)| {
                    table_rank(a).cmp(&table_rank(b)).then_with(|| a.cmp(b))
                });
                let share_lines = shares
                    .iter()
                    .map(|(table, share)| {
                        let per_worker = data
                            .tables
                            .get(*table)
                            .and_then(|t| t.get("shards_per_worker").copied());
                        let owned = owned_shards(share.get("index").copied(), per_worker);
                        let assigned = share.get("assigned_shards").copied().unwrap_or(0.0);
                        let positions = share.get("populated_positions").copied().unwrap_or(0.0);
                        format!(
                            "<p class=\"kv\"><b>{table}</b> {owned} &middot; {assigned} assigned \
&middot; {positions} positions</p>",
                            table = escape(table),
                            assigned = format_number(assigned),
                            positions = format_number(positions),
                        )
                    })
                    .collect::<String>();
                let ram = worker_ram_line(gauges);
                format!(
                    "<div class=\"leaf\"><div class=\"node {tone}\"><p class=\"role\">Worker</p>\
<p class=\"id\">{name}</p>\
<p class=\"desc\">Holds sealed iPIR artifacts for its shards and evaluates its slice of every query.</p>\
{share_lines}{ram}\
<span class=\"state\">{state}</span></div></div>",
                    name = escape(name),
                )
            })
            .collect::<String>()
    };

    let tables = tables_section(data);

    format!(
        "<section class=\"card\"><p class=\"eyebrow\">Fleet topology</p>\
<div class=\"topo\">\
<div class=\"node\"><p class=\"role\">Chain source</p><p class=\"id\">zakurad archive node</p>\
<p class=\"desc\">Same host, loopback RPC. The only source of finalized blocks and the Ironwood tree size.</p></div>\
<div class=\"trunk\"></div>\
<div class=\"node coord\"><p class=\"role\">Coordinator</p><p class=\"id\">{hostname}</p>\
<p class=\"desc\">Ingests finalized outputs into the Enhance journal, publishes one generation per block, and fans each query out to the workers. Clients enter over HTTPS through Caddy.</p>\
<p class=\"meta\">{coordinator_meta}</p>{coordinator_meta2}</div>\
<div class=\"trunk\"></div>\
<div class=\"bus\">{leaves}</div>{tables}</div>\
<p class=\"note\">Workers are probed by the coordinator on each scrape over the private network; \
only their inventory names are shown. A worker whose generation trails the coordinator's is still \
serving the previous snapshot.</p></section>",
        hostname = escape(&data.hostname),
        coordinator_meta2 = if coordinator_meta2.is_empty() {
            String::new()
        } else {
            format!("<p class=\"meta\">{coordinator_meta2}</p>")
        },
    )
}

/// `RAM 1.1 GiB used of 62.8 GiB · rss 0.9 GiB`, graded like the host card.
fn worker_ram_line(gauges: &BTreeMap<String, f64>) -> String {
    let (Some(total), Some(available)) = (
        gauges.get("total_memory_bytes").copied(),
        gauges.get("available_memory_bytes").copied(),
    ) else {
        return String::new();
    };
    if total <= 0.0 {
        return String::new();
    }
    let rss_suffix = gauges
        .get("process_rss_bytes")
        .filter(|value| **value > 0.0)
        .map(|value| format!(" &middot; rss {}", bytes_human(*value as u64)))
        .unwrap_or_default();
    if available <= 0.0 {
        // Platforms that cannot report availability still get the total.
        return format!(
            "<p class=\"meta\">RAM {} total{rss_suffix}</p>",
            bytes_human(total as u64)
        );
    }
    let used = (total - available).max(0.0);
    let tone = if (available as u64) < thresholds::MEMORY_AVAILABLE_BYTES {
        " bad"
    } else if (available as u64) < thresholds::MEMORY_AVAILABLE_BYTES * 2 {
        " warn"
    } else {
        ""
    };
    format!(
        "<p class=\"meta{tone}\">RAM {} used of {}{rss_suffix}</p>",
        bytes_human(used as u64),
        bytes_human(total as u64),
    )
}

/// One compact card per PIR table the coordinator knows about. Planned tables
/// (registered = 0) are drawn dimmed with just their layout.
fn tables_section(data: &DashboardData) -> String {
    let tables = ordered_tables(data);
    if tables.is_empty() {
        return String::new();
    }
    let cards = tables
        .iter()
        .map(|(name, gauges)| {
            let g = |key: &str| gauges.get(key).copied();
            let registered = g("registered").is_some_and(|v| v >= 1.0);
            let layout = match (
                g("records_per_row"),
                g("record_bytes"),
                g("shard_rows"),
                g("shard_positions"),
            ) {
                (Some(rpr), Some(rb), Some(rows), Some(positions)) => format!(
                    "<p class=\"kv\">{rpr} &times; {rb} B per row &middot; {rows} rows per shard \
&middot; <b>{positions}</b> positions per shard</p>",
                    rpr = format_number(rpr),
                    rb = format_number(rb),
                    rows = format_number(rows),
                    positions = format_number(positions),
                ),
                _ => String::new(),
            };
            if !registered {
                return format!(
                    "<div class=\"node planned\"><p class=\"role\">Table</p><p class=\"id\">{name}</p>\
{layout}<span class=\"state\">planned &middot; not registered</span></div>",
                    name = escape(name),
                );
            }
            let shards = g("shards").unwrap_or(0.0);
            let sealed = g("sealed_shards").unwrap_or(0.0);
            let positions = g("positions").unwrap_or(0.0);
            let pool = g("pool_workers").unwrap_or(0.0);
            let slots = g("query_slots_available").unwrap_or(0.0);
            let usage = format!(
                "<p class=\"kv\"><b>{shards}</b> shards ({sealed} sealed) &middot; <b>{positions}</b> \
positions &middot; {pool} workers &middot; {slots} query slots free</p>",
                shards = format_number(shards),
                sealed = format_number(sealed),
                positions = format_number(positions),
                pool = format_number(pool),
                slots = format_number(slots),
            );
            let per_worker = g("shards_per_worker");
            let mut holders: Vec<(String, f64)> = data
                .worker_tables
                .iter()
                .filter_map(|(worker, tables)| {
                    tables
                        .get(*name)
                        .and_then(|share| share.get("index").copied())
                        .map(|index| (worker.clone(), index))
                })
                .collect();
            holders.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
            let holders = holders
                .iter()
                .map(|(worker, index)| {
                    format!(
                        "{}: {}",
                        escape(worker),
                        owned_shards(Some(*index), per_worker)
                    )
                })
                .collect::<Vec<_>>()
                .join(" &middot; ");
            let holders = if holders.is_empty() {
                String::new()
            } else {
                format!("<p class=\"kv\">{holders}</p>")
            };
            format!(
                "<div class=\"node is-ok\"><p class=\"role\">Table</p><p class=\"id\">{name}</p>\
{layout}{usage}{holders}{capacity}</div>",
                name = escape(name),
                capacity = table_capacity_meter(gauges),
            )
        })
        .collect::<String>();
    format!("<p class=\"section\">Tables</p><div class=\"tables\">{cards}</div>")
}

/// How full one table is: positions published against
/// `pool_workers × shards_per_worker × shard_positions`.
fn table_capacity_meter(gauges: &BTreeMap<String, f64>) -> String {
    let (Some(pool), Some(per_worker), Some(shard_positions), Some(positions)) = (
        gauges.get("pool_workers").copied(),
        gauges.get("shards_per_worker").copied(),
        gauges.get("shard_positions").copied(),
        gauges.get("positions").copied(),
    ) else {
        return String::new();
    };
    if pool <= 0.0 || per_worker <= 0.0 || shard_positions <= 0.0 {
        return String::new();
    }
    let shard_capacity = pool * per_worker;
    let position_capacity = shard_capacity * shard_positions;
    let ratio = positions / position_capacity;
    let shards_used = (positions / shard_positions).ceil();
    let tone = if ratio > 0.90 {
        "bad"
    } else if ratio > 0.75 {
        "warn"
    } else {
        ""
    };
    meter(
        "Capacity",
        &format!(
            "{} of {} positions &middot; {} of {} shards &middot; append a worker before this fills",
            format_number(positions),
            format_number(position_capacity),
            format_number(shards_used),
            format_number(shard_capacity),
        ),
        ratio,
        tone,
    )
}

/// Plain-language summary of how the fleet works. Every number comes from the
/// `<prefix>_table_*` and `<prefix>_layout_*` gauges, so the text cannot drift
/// from the binary. Rendered only when the coordinator exports tables.
fn explainer_card(data: &DashboardData) -> String {
    let tables = ordered_tables(data);
    if tables.is_empty() {
        return String::new();
    }
    let chain = |name: &str| {
        data.layout
            .get(name)
            .map(|value| format_number(*value))
            .unwrap_or_else(|| "?".to_string())
    };
    let layout_rows = tables
        .iter()
        .map(|(name, g)| {
            let n = |key: &str| g.get(key).map(|v| format_number(*v)).unwrap_or_else(|| "?".into());
            let state = if g.get("registered").is_some_and(|v| *v >= 1.0) {
                "serving"
            } else {
                "planned"
            };
            format!(
                "<tr><th>{name}</th><td>{rpr} &times; {rb} B</td><td>{rows}</td><td>{positions}</td>\
<td class=\"muted\">{state}</td></tr>",
                name = escape(name),
                rpr = n("records_per_row"),
                rb = n("record_bytes"),
                rows = n("shard_rows"),
                positions = n("shard_positions"),
            )
        })
        .collect::<String>();
    let per_worker = tables
        .iter()
        .find_map(|(_, g)| g.get("shards_per_worker").copied())
        .unwrap_or(0.0);
    let ownership = if per_worker >= 1.0 {
        format!(
            "Worker <b>n</b> of a table's pool owns shards <b>n&times;{spw}</b> to \
<b>n&times;{spw}+{last}</b>; pools are append-only, so adding a worker never moves a shard.",
            spw = format_number(per_worker),
            last = format_number(per_worker - 1.0),
        )
    } else {
        "Each worker owns a fixed range of shard ids; pools are append-only.".to_string()
    };
    format!(
        "<section class=\"card\"><p class=\"eyebrow\">How this fleet works</p>\
<dl class=\"explain\">\
<dt>Ingest</dt><dd>The coordinator alone reads the chain. It polls the local archive node, waits \
<b>{confirmations}</b> confirmations, and appends each finalized block to one append-only journal \
per table, starting at Ironwood activation, height <b>{activation}</b>. Workers never touch the chain.</dd>\
<dt>Generations</dt><dd>Every finalized block publishes a new generation, named by its height, whose \
manifest describes every table at that one anchor. A wallet pins one generation for a whole sync \
pass. The eight newest generations stay answerable, so a query built just before a publish still \
succeeds. Only shards whose rows changed are rebuilt; sealed shards are reused from the worker's \
cache.</dd>\
<dt>Layout</dt><dd>Every table is indexed directly by position. {ownership}\
<div class=\"wrap\"><table><thead><tr><th>Table</th><th>Row</th><th>Rows / shard</th>\
<th>Positions / shard</th><th></th></tr></thead><tbody>{layout_rows}</tbody></table></div></dd>\
<dt>Queries</dt><dd>A client fetches a table's public parameters and sends one query for the whole \
table to <code>/v1/&lt;table&gt;/query</code>. The coordinator slices it per shard, fans the slices \
out to that table's workers in parallel, sums the partial answers, and packs a single response. \
Admission is per table; excess queries are shed with a 503.</dd>\
</dl></section>",
        confirmations = chain("confirmations"),
        activation = chain("activation_height"),
    )
}

fn phase_label(code: f64) -> &'static str {
    match code as i64 {
        0 => "syncing",
        1 => "building",
        2 => "serving",
        3 => "failed",
        _ => "unknown",
    }
}

fn service_card(data: &DashboardData) -> String {
    let mut rows = String::new();
    if data.snapshot_gauges.is_empty() {
        rows.push_str("<li><span class=\"k\">Snapshot metrics</span><span class=\"v muted\">none yet</span></li>");
    } else {
        for (name, value) in &data.snapshot_gauges {
            rows.push_str(&format!(
                "<li><span class=\"k\" title=\"{raw}\">{name}</span>\
<span class=\"v\">{value}</span></li>",
                raw = escape(name),
                name = escape(&humanize_gauge(name, &data.schema.gauge_prefix)),
                value = format_number(*value),
            ));
        }
    }
    let memory = data
        .process_resident_memory_bytes
        .map(|bytes| bytes_human(bytes as u64))
        .unwrap_or_else(|| "unavailable".to_string());
    rows.push_str(&format!(
        "<li><span class=\"k\">Resident memory</span><span class=\"v\">{memory}</span></li>"
    ));
    let health_body = data
        .health_body
        .as_deref()
        .map(escape)
        .unwrap_or_else(|| "unavailable".to_string());
    format!(
        "<section class=\"card\"><p class=\"eyebrow\">Service &amp; snapshot</p>\
<ul class=\"rows\">{rows}</ul>\
<p class=\"note\"><strong>/health</strong> <code>{health_body}</code></p></section>"
    )
}

fn host_card(host: &HostHealth) -> String {
    let memory_used = if host.total_memory_bytes > 0 {
        1.0 - host.available_memory_bytes as f64 / host.total_memory_bytes as f64
    } else {
        0.0
    };
    let memory_tone = if host.available_memory_bytes < thresholds::MEMORY_AVAILABLE_BYTES {
        "bad"
    } else if host.available_memory_bytes < thresholds::MEMORY_AVAILABLE_BYTES * 2 {
        "warn"
    } else {
        ""
    };
    let disk_tone = if host.disk_used_ratio > thresholds::DISK_USED_RATIO {
        "bad"
    } else if host.disk_used_ratio > 0.80 {
        "warn"
    } else {
        ""
    };
    format!(
        "<section class=\"card\"><p class=\"eyebrow\">Host</p>\
{memory}{disk}\
<ul class=\"rows\">\
<li><span class=\"k\">Load 1 / 5 / 15</span>\
<span class=\"v\">{load_one:.2} · {load_five:.2} · {load_fifteen:.2}</span></li>\
<li><span class=\"k\">Data directory</span><span class=\"v wrap-any\">{data_dir}</span></li>\
</ul></section>",
        memory = meter(
            "Enhancery",
            &format!(
                "{} free of {}",
                bytes_human(host.available_memory_bytes),
                bytes_human(host.total_memory_bytes)
            ),
            memory_used,
            memory_tone,
        ),
        disk = meter(
            "Disk",
            &format!(
                "{} free of {}",
                bytes_human(host.disk_available_bytes),
                bytes_human(host.disk_total_bytes)
            ),
            host.disk_used_ratio,
            disk_tone,
        ),
        load_one = host.load_one,
        load_five = host.load_five,
        load_fifteen = host.load_fifteen,
        data_dir = escape(&host.data_dir.display().to_string()),
    )
}

fn meter(label: &str, detail: &str, ratio: f64, tone: &str) -> String {
    let percent = (ratio * 100.0).clamp(0.0, 100.0);
    format!(
        "<div class=\"meter\"><div class=\"meter-head\"><span class=\"k\">{label}</span>\
<span class=\"v{tone_class}\">{percent:.0}% used</span></div>\
<div class=\"bar\"><i class=\"{tone}\" style=\"width:{percent:.1}%\"></i></div>\
<p class=\"meter-foot\">{detail}</p></div>",
        tone_class = class_suffix(tone),
    )
}

fn active_alerts_card(alerts: &[Alert]) -> String {
    if alerts.is_empty() {
        return "<section class=\"card\"><p class=\"eyebrow\">Active alerts</p>\
<ul class=\"alerts\"><li class=\"is-ok\"><p class=\"name\">All checks passing</p>\
<p class=\"detail\">No thresholds are currently breached.</p></li></ul></section>"
            .to_string();
    }
    let items = alerts
        .iter()
        .map(|alert| {
            format!(
                "<li class=\"is-bad\"><p class=\"name\">{check}</p>\
<p class=\"detail\">{observed} <span class=\"muted\">(threshold {threshold})</span></p>\
<p class=\"when\">firing {since}</p></li>",
                check = escape(&alert.check),
                observed = escape(&alert.observed),
                threshold = escape(&alert.threshold),
                since = relative_time(alert.fired_at),
            )
        })
        .collect::<String>();
    format!(
        "<section class=\"card\"><p class=\"eyebrow\">Active alerts</p>\
<ul class=\"alerts\">{items}</ul></section>"
    )
}

fn recent_alerts_card(recent: &[(SystemTime, String)]) -> String {
    if recent.is_empty() {
        return "<section class=\"card\"><p class=\"eyebrow\">Alert history</p>\
<p class=\"empty\">Nothing recorded since this sidecar started.</p></section>"
            .to_string();
    }
    let items = recent
        .iter()
        .map(|(at, message)| {
            format!(
                "<li><span class=\"k\">{message}</span><span class=\"v\">{when}</span></li>",
                message = escape(message),
                when = relative_time(*at),
            )
        })
        .collect::<String>();
    format!(
        "<section class=\"card\"><p class=\"eyebrow\">Alert history</p>\
<ul class=\"rows\">{items}</ul></section>"
    )
}

fn privacy_note() -> String {
    format!(
        "<p class=\"note\"><strong>Privacy.</strong> This sidecar scrapes only aggregate, \
allowlisted PIR metrics and local host health. It does not collect request bodies, positions, \
IP addresses, headers, query identifiers, or any other per-user data. Every value on this page is \
server-rendered; the browser polls this dashboard every {REFRESH_SECONDS} seconds and never \
contacts the PIR server or its <code>/metrics</code> endpoint.</p>"
    )
}

/// Turns a raw gauge name like `enhance_snapshot_age_seconds` into "Age (seconds)".
/// Callers keep the original name as a tooltip so an operator can still map a
/// row back to its Prometheus series.
fn humanize_gauge(name: &str, gauge_prefix: &str) -> String {
    let stem = name.strip_prefix(gauge_prefix).unwrap_or(name);
    let (stem, unit) = ["seconds", "bytes", "ratio"]
        .iter()
        .find_map(|unit| {
            stem.strip_suffix(&format!("_{unit}"))
                .filter(|rest| !rest.is_empty())
                .map(|rest| (rest, Some(*unit)))
        })
        .unwrap_or((stem, None));
    let mut label = stem.replace('_', " ");
    if let Some(first) = label.get_mut(0..1) {
        first.make_ascii_uppercase();
    }
    match unit {
        Some(unit) => format!("{label} ({unit})"),
        None => label,
    }
}

fn format_number(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        format!("{value:.3}")
    }
}

fn bytes_human(bytes: u64) -> String {
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    if bytes as f64 >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB)
    } else {
        format!("{:.1} MiB", bytes as f64 / MIB)
    }
}

fn time(value: SystemTime) -> String {
    let seconds = value
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    chrono::DateTime::from_timestamp(seconds as i64, 0)
        .map(|value| value.to_rfc3339())
        .unwrap_or_else(|| seconds.to_string())
}

/// Human "3s ago" style age. Clock skew or a future timestamp collapses to
/// "just now" rather than rendering a negative duration.
fn relative_time(value: SystemTime) -> String {
    let Ok(elapsed) = SystemTime::now().duration_since(value) else {
        return "just now".to_string();
    };
    let seconds = elapsed.as_secs();
    match seconds {
        0..=4 => "just now".to_string(),
        5..=59 => format!("{seconds}s ago"),
        60..=3599 => format!("{}m ago", seconds / 60),
        3600..=86399 => format!("{}h ago", seconds / 3600),
        _ => format!("{}d ago", seconds / 86400),
    }
}

fn escape(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn sample() -> DashboardData {
        let mut data = DashboardData::new(
            "Enhance PIR APM".to_string(),
            Schema::enhance_default(),
            "staging".to_string(),
            "pir-primary".to_string(),
            HostHealth {
                load_one: 0.4,
                load_five: 0.5,
                load_fifteen: 0.6,
                total_memory_bytes: 16 * 1024 * 1024 * 1024,
                available_memory_bytes: 8 * 1024 * 1024 * 1024,
                disk_total_bytes: 200 * 1024 * 1024 * 1024,
                disk_available_bytes: 100 * 1024 * 1024 * 1024,
                disk_used_ratio: 0.5,
                data_dir: "/srv/zakura/enhance-data".into(),
            },
        );
        data.scrape_error = None;
        data.last_scrape = Some(SystemTime::now());
        data.health_status = Some(200);
        data.ready_status = Some(200);
        data.endpoints.insert(
            "metadata".to_string(),
            EndpointWindow {
                qps: 1.5,
                requests: 450.0,
                errors_5xx: 0.0,
                error_ratio: 0.0,
                observed: LatencyWindow {
                    samples: 450.0,
                    p50: Some(0.01),
                    p95: Some(0.05),
                    p99: Some(0.09),
                },
                in_flight: 2.0,
                ..Default::default()
            },
        );
        data
    }

    #[test]
    fn renders_a_complete_document() {
        let html = render(&sample());
        assert!(html.starts_with("<!doctype html>"));
        assert!(html.ends_with("</html>"));
        assert!(html.contains("id=\"app\""));
        assert!(html.contains("<title>Enhance PIR APM</title>"));
        assert!(html.contains("<h1>Enhance PIR APM</h1>"));
    }

    #[test]
    fn every_allowlisted_endpoint_gets_a_row_even_without_data() {
        let html = render(&sample());
        for endpoint in &Schema::enhance_default().endpoints {
            assert!(
                html.contains(&format!("<th>{endpoint}</th>")),
                "missing row for {endpoint}"
            );
        }
    }

    #[test]
    fn hostile_values_are_escaped() {
        let mut data = sample();
        data.hostname = "<script>alert(1)</script>".to_string();
        data.scrape_error = Some("bad \"quote\" & <tag>".to_string());
        let html = render(&data);
        assert!(!html.contains("<script>alert(1)</script>"));
        assert!(html.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
        assert!(html.contains("&quot;quote&quot;"));
    }

    #[test]
    fn latency_is_graded_against_the_alert_budget() {
        let budget = latency_budget(&Schema::enhance_default(), "metadata");
        assert!(latency_cell(Some(0.01), budget).contains("class=\"\""));
        assert!(latency_cell(Some(0.7), budget).contains("warn"));
        assert!(latency_cell(Some(1.5), budget).contains("bad"));
        assert!(latency_cell(None, budget).contains("—"));
    }

    #[test]
    fn latency_budgets_follow_endpoint_kind() {
        let schema = Schema::enhance_default();
        // Discovered endpoints get the default budget; informational and
        // processing endpoints get none in the observed table.
        assert_eq!(latency_budget(&schema, "nope"), Some(1.0));
        assert!(latency_budget(&schema, "health").is_none());
        assert!(latency_budget(&schema, "query").is_none());
        assert_eq!(latency_budget(&schema, "init"), Some(2.0));
        assert!(!latency_cell(Some(99.0), None).contains("bad"));
    }

    #[test]
    fn processing_split_keeps_body_receive_timing_off_the_public_dashboard() {
        let mut data = sample();
        data.endpoints.insert(
            "query".to_string(),
            EndpointWindow {
                observed: LatencyWindow {
                    samples: 20.0,
                    p99: Some(10.0),
                    ..Default::default()
                },
                processing: LatencyWindow {
                    samples: 20.0,
                    p99: Some(6.0),
                    ..Default::default()
                },
                in_flight: 3.0,
                processing_in_flight: 1.0,
                ..Default::default()
            },
        );
        let html = render(&data);
        assert!(html.contains("query latency split"));
        assert!(!html.contains("metadata latency split"));
        assert!(!html.contains("Body receive (upload proxy)"));
        assert!(html.contains("Server processing"));
        assert!(html.contains("only distribution evaluated against the 5.000s"));
        assert!(html.contains("Body-receive timing is not rendered here"));
        assert_eq!(html.matches("<td class=\"bad\">").count(), 1);
    }

    #[test]
    fn a_count_that_displays_as_zero_is_not_flagged_as_an_error() {
        let window = EndpointWindow {
            errors_5xx: 0.2,
            error_ratio: 0.001,
            ..EndpointWindow::default()
        };
        let cell = error_cell(&window);
        assert!(
            cell.contains("muted"),
            "sub-half count should read as clean"
        );
        assert!(!cell.contains("bad"));

        let mut data = sample();
        data.endpoints.insert("metadata".to_string(), window);
        assert!(!render(&data).contains("figure bad"));
    }

    #[test]
    fn long_health_bodies_and_paths_are_allowed_to_wrap() {
        let mut data = sample();
        data.health_body =
            Some("{\"phase\":{\"phase\":\"serving\"},\"anchor_height\":3470117}".into());
        data.host.data_dir = "/srv/zakura/enhance-data".into();
        let html = render(&data);
        assert!(html.contains(".note code{display:block"));
        assert!(html.contains("overflow-wrap:anywhere"));
        assert!(html.contains("<span class=\"v wrap-any\">/srv/zakura/enhance-data</span>"));
        assert!(html.contains("<code>{&quot;phase&quot;"));
    }

    fn fleet_sample() -> DashboardData {
        let mut data = sample();
        data.snapshot_gauges
            .insert("enhance_snapshot_phase_code".into(), 2.0);
        data.snapshot_gauges
            .insert("enhance_snapshot_generation".into(), 100.0);
        data.snapshot_gauges
            .insert("enhance_snapshot_anchor_height".into(), 100.0);
        data.snapshot_gauges
            .insert("enhance_snapshot_retained_generations".into(), 2.0);
        data.snapshot_gauges
            .insert("enhance_snapshot_ironwood_tree_size".into(), 150_124.0);
        data.layout.insert("confirmations".into(), 10.0);
        data.layout.insert("activation_height".into(), 3_428_143.0);
        let enhance = BTreeMap::from([
            ("registered".to_string(), 1.0),
            ("records_per_row".to_string(), 9.0),
            ("record_bytes".to_string(), 724.0),
            ("shard_rows".to_string(), 8_192.0),
            ("shard_positions".to_string(), 73_728.0),
            ("shards_per_worker".to_string(), 2.0),
            ("pool_workers".to_string(), 2.0),
            ("query_slots_available".to_string(), 2.0),
            ("positions".to_string(), 150_124.0),
            ("shards".to_string(), 3.0),
            ("sealed_shards".to_string(), 2.0),
        ]);
        let witness = BTreeMap::from([
            ("registered".to_string(), 0.0),
            ("records_per_row".to_string(), 256.0),
            ("record_bytes".to_string(), 32.0),
            ("shard_rows".to_string(), 8_192.0),
            ("shard_positions".to_string(), 2_097_152.0),
            ("shards_per_worker".to_string(), 2.0),
        ]);
        data.tables.insert("witness".into(), witness);
        data.tables.insert("enhance".into(), enhance);
        data.workers.insert(
            "worker-b".into(),
            BTreeMap::from([
                ("up".to_string(), 1.0),
                ("index".to_string(), 0.0),
                (
                    "total_memory_bytes".to_string(),
                    64.0 * 1024.0 * 1024.0 * 1024.0,
                ),
                (
                    "available_memory_bytes".to_string(),
                    60.0 * 1024.0 * 1024.0 * 1024.0,
                ),
                ("process_rss_bytes".to_string(), 1024.0 * 1024.0 * 1024.0),
            ]),
        );
        data.workers.insert(
            "worker-a".into(),
            BTreeMap::from([("up".to_string(), 1.0), ("index".to_string(), 1.0)]),
        );
        data.worker_tables.insert(
            "worker-b".into(),
            BTreeMap::from([(
                "enhance".to_string(),
                BTreeMap::from([
                    ("index".to_string(), 0.0),
                    ("assigned_shards".to_string(), 2.0),
                    ("populated_positions".to_string(), 147_456.0),
                ]),
            )]),
        );
        data.worker_tables.insert(
            "worker-a".into(),
            BTreeMap::from([(
                "enhance".to_string(),
                BTreeMap::from([
                    ("index".to_string(), 1.0),
                    ("assigned_shards".to_string(), 1.0),
                    ("populated_positions".to_string(), 2_668.0),
                ]),
            )]),
        );
        data
    }

    #[test]
    fn fleet_card_draws_each_worker_with_its_health() {
        let mut data = fleet_sample();
        data.workers
            .insert("worker-c".into(), BTreeMap::from([("up".to_string(), 0.0)]));
        data.workers.insert(
            "worker-d".into(),
            BTreeMap::from([("up".to_string(), 1.0), ("generation".to_string(), 99.0)]),
        );
        let html = render(&data);
        assert!(html.contains("Fleet topology"));
        assert!(html.contains("serving &middot; anchor 100 &middot; generation 100"));
        assert!(html.contains("2 generations answerable"));
        assert!(html.contains("zakurad archive node"));
        assert!(html.contains("Ingests finalized outputs"));
        assert!(html.contains("Holds sealed iPIR artifacts"));
        assert_eq!(html.matches("class=\"node is-ok\"").count(), 3); // 2 workers + Enhance table
        assert_eq!(html.matches("class=\"node is-bad\"").count(), 1);
        assert_eq!(html.matches("class=\"node is-warn\"").count(), 1);
        assert!(html.contains("unreachable"));
        assert!(html.contains("generation lag"));
        assert!(render(&sample()).contains("none reported yet"));
    }

    #[test]
    fn workers_are_ordered_by_index_and_list_per_table_shares() {
        let html = render(&fleet_sample());
        let b = html.find("<p class=\"id\">worker-b</p>").unwrap();
        let a = html.find("<p class=\"id\">worker-a</p>").unwrap();
        assert!(b < a, "index 0 must render before index 1");
        assert!(html.contains(
            "<b>enhance</b> shards 0&ndash;1 &middot; 2 assigned &middot; 147456 positions"
        ));
        assert!(html.contains(
            "<b>enhance</b> shards 2&ndash;3 &middot; 1 assigned &middot; 2668 positions"
        ));
    }

    #[test]
    fn worker_ram_line_renders_when_memory_gauges_exist() {
        let html = render(&fleet_sample());
        assert!(html.contains("RAM 4.00 GiB used of 64.00 GiB &middot; rss 1.00 GiB"));
        // worker-a has no memory gauges: exactly one RAM line on the page.
        assert_eq!(html.matches("RAM ").count(), 1);

        let mut low = BTreeMap::new();
        low.insert(
            "total_memory_bytes".to_string(),
            8.0 * 1024.0 * 1024.0 * 1024.0,
        );
        low.insert(
            "available_memory_bytes".to_string(),
            100.0 * 1024.0 * 1024.0,
        );
        assert!(worker_ram_line(&low).contains("meta bad"));

        let mut unknown = BTreeMap::new();
        unknown.insert(
            "total_memory_bytes".to_string(),
            8.0 * 1024.0 * 1024.0 * 1024.0,
        );
        unknown.insert("available_memory_bytes".to_string(), 0.0);
        assert!(worker_ram_line(&unknown).contains("RAM 8.00 GiB total"));
    }

    #[test]
    fn tables_render_in_fixed_order_with_planned_ones_dimmed() {
        let html = render(&fleet_sample());
        let enhance = html.find("<p class=\"id\">enhance</p>").unwrap();
        let witness = html.find("<p class=\"id\">witness</p>").unwrap();
        assert!(enhance < witness);
        assert!(html.contains("9 &times; 724 B per row &middot; 8192 rows per shard"));
        assert!(html.contains("<b>3</b> shards (2 sealed) &middot; <b>150124</b> positions"));
        assert!(html.contains("worker-b: shards 0&ndash;1 &middot; worker-a: shards 2&ndash;3"));
        assert_eq!(html.matches("class=\"node planned\"").count(), 1);
        assert!(html.contains("planned &middot; not registered"));
        assert!(html.contains("2097152</b> positions per shard"));
    }

    #[test]
    fn table_capacity_meter_uses_pool_and_layout() {
        let data = fleet_sample();
        let html = render(&data);
        assert!(html.contains("150124 of 294912 positions &middot; 3 of 4 shards"));
        let mut enhance = data.tables["enhance"].clone();
        assert!(!table_capacity_meter(&enhance).contains("warn"));
        enhance.insert("positions".to_string(), 240_000.0);
        assert!(table_capacity_meter(&enhance).contains("warn"));
        enhance.insert("positions".to_string(), 285_000.0);
        assert!(table_capacity_meter(&enhance).contains("bad"));
        // A planned table has no pool: no meter.
        assert!(table_capacity_meter(&data.tables["witness"]).is_empty());
    }

    #[test]
    fn explainer_lists_every_table_and_hides_without_them() {
        let html = render(&fleet_sample());
        assert!(html.contains("How this fleet works"));
        assert!(html.contains("<b>10</b> confirmations"));
        assert!(html.contains("height <b>3428143</b>"));
        assert!(html.contains("n&times;2+1"));
        assert!(html
            .contains("<tr><th>enhance</th><td>9 &times; 724 B</td><td>8192</td><td>73728</td>"));
        assert!(html.contains("<tr><th>witness</th><td>256 &times; 32 B</td>"));
        assert!(html.contains("eight newest generations stay answerable"));
        assert!(!render(&sample()).contains("How this fleet works"));
    }

    #[test]
    fn discovered_endpoints_appear_after_configured_ones() {
        let mut data = sample();
        data.endpoints.insert(
            "witness_query".to_string(),
            EndpointWindow {
                qps: 0.5,
                requests: 10.0,
                processing: LatencyWindow {
                    samples: 10.0,
                    p99: Some(0.2),
                    ..Default::default()
                },
                processing_available: true,
                ..Default::default()
            },
        );
        let html = render(&data);
        let query = html.find("<th>query</th>").unwrap();
        let witness = html.find("<th>witness_query</th>").unwrap();
        assert!(query < witness);
        assert!(html.contains("witness_query latency split"));
    }

    #[test]
    fn meters_stay_within_the_track() {
        assert!(meter("Disk", "x", 1.8, "bad").contains("width:100.0%"));
        assert!(meter("Disk", "x", -0.5, "").contains("width:0.0%"));
    }

    #[test]
    fn gauge_names_become_readable_labels() {
        let prefix = "enhance_snapshot_";
        assert_eq!(
            humanize_gauge("enhance_snapshot_anchor_height", prefix),
            "Anchor height"
        );
        assert_eq!(
            humanize_gauge("enhance_snapshot_query_slots_available", prefix),
            "Query slots available"
        );
        assert_eq!(
            humanize_gauge("enhance_snapshot_age_seconds", prefix),
            "Age (seconds)"
        );
        assert_eq!(
            humanize_gauge("unprefixed_gauge", prefix),
            "Unprefixed gauge"
        );
        // A name that is nothing but a unit keeps its stem rather than
        // collapsing to a bare "()".
        assert_eq!(
            humanize_gauge("enhance_snapshot_seconds", prefix),
            "Seconds"
        );
    }

    #[test]
    fn the_raw_gauge_name_survives_as_a_tooltip() {
        let mut data = sample();
        data.snapshot_gauges
            .insert("enhance_snapshot_generation".into(), 42.0);
        let html = render(&data);
        assert!(html.contains("title=\"enhance_snapshot_generation\""));
        assert!(html.contains(">Generation<"));
    }

    #[test]
    fn relative_time_reads_naturally() {
        let now = SystemTime::now();
        assert_eq!(relative_time(now), "just now");
        assert_eq!(relative_time(now - Duration::from_secs(30)), "30s ago");
        assert_eq!(relative_time(now - Duration::from_secs(600)), "10m ago");
        assert_eq!(relative_time(now + Duration::from_secs(60)), "just now");
    }

    #[test]
    fn first_paint_before_any_scrape_is_renderable() {
        let data = DashboardData::new(
            "Enhance PIR APM".to_string(),
            Schema::enhance_default(),
            "unknown".to_string(),
            "host".to_string(),
            HostHealth::default(),
        );
        let html = render(&data);
        assert!(html.contains("Awaiting first scrape"));
        assert!(html.contains("waiting for first scrape"));
    }
}
