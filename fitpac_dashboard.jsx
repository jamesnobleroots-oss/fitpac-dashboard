import React, { useState, useEffect, useMemo } from "react";
import {
  Activity,
  AlertTriangle,
  ShieldCheck,
  ShieldAlert,
  Zap,
  TrendingUp,
  Droplets,
  Bot,
  BadgeCheck,
  Flame,
  Radio,
  Clock,
  RefreshCw,
  Eye,
  Target,
  Skull,
} from "lucide-react";
import {
  RadialBarChart,
  RadialBar,
  PolarAngleAxis,
  ResponsiveContainer,
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
} from "recharts";

// --- MOCK FITPAC PAYLOADS (matching backend schema exactly) ---
const PAYLOADS = {
  ignition: {
    timestamp: "2026-04-17T19:44:47.449370",
    ticker: "$PEPE",
    signal_status: "VIRAL_IGNITION",
    confidence_score: 0.82,
    social_metrics: {
      hype_velocity: 0.88,
      authenticity_score: 0.85,
      influence_weight: 4.9,
      vip_triggers: ["@CryptoWhale"],
    },
    chain_metrics: {
      insider_distribution_ratio: 0.15,
      liquidity_depth_usd: 250000,
      hard_veto_active: false,
    },
    system_warnings: [],
  },
  standby: {
    timestamp: "2026-04-17T19:42:12.220000",
    ticker: "$WOJAK",
    signal_status: "STANDBY",
    confidence_score: 0.41,
    social_metrics: {
      hype_velocity: 0.32,
      authenticity_score: 0.61,
      influence_weight: 1.4,
      vip_triggers: [],
    },
    chain_metrics: {
      insider_distribution_ratio: 0.22,
      liquidity_depth_usd: 95000,
      hard_veto_active: false,
    },
    system_warnings: [],
  },
  vetoed: {
    timestamp: "2026-04-17T19:40:03.772000",
    ticker: "$RUGME",
    signal_status: "VETOED",
    confidence_score: 0.0,
    social_metrics: {
      hype_velocity: 0.94,
      authenticity_score: 0.88,
      influence_weight: 5.6,
      vip_triggers: ["@VitalikButerin", "@AlphaCallerX"],
    },
    chain_metrics: {
      insider_distribution_ratio: 0.58,
      liquidity_depth_usd: 42000,
      hard_veto_active: true,
    },
    system_warnings: ["EXIT LIQUIDITY TRAP DETECTED: Insiders are distributing."],
  },
};

// --- HELPERS ---
const fmtUSD = (n) =>
  "$" +
  (n >= 1_000_000
    ? (n / 1_000_000).toFixed(2) + "M"
    : n >= 1_000
    ? (n / 1_000).toFixed(1) + "K"
    : n.toString());

const fmtPct = (n) => (n * 100).toFixed(1) + "%";

const statusStyle = (status) => {
  if (status === "VIRAL_IGNITION")
    return {
      text: "text-emerald-300",
      bg: "bg-emerald-500/15",
      ring: "ring-emerald-400/40",
      dot: "bg-emerald-400",
      icon: Flame,
      label: "VIRAL IGNITION",
    };
  if (status === "VETOED")
    return {
      text: "text-red-300",
      bg: "bg-red-500/15",
      ring: "ring-red-400/40",
      dot: "bg-red-500",
      icon: Skull,
      label: "VETOED",
    };
  return {
    text: "text-amber-200",
    bg: "bg-amber-500/10",
    ring: "ring-amber-400/30",
    dot: "bg-amber-400",
    icon: Radio,
    label: "STANDBY",
  };
};

// --- UI PRIMITIVES ---
const Card = ({ children, className = "" }) => (
  <div
    className={
      "rounded-2xl border border-white/5 bg-slate-900/60 backdrop-blur-md shadow-2xl shadow-black/30 " +
      className
    }
  >
    {children}
  </div>
);

const Pill = ({ children, tone = "slate" }) => {
  const tones = {
    slate: "bg-slate-800/70 text-slate-200 ring-slate-600/40",
    emerald: "bg-emerald-500/10 text-emerald-300 ring-emerald-400/30",
    red: "bg-red-500/10 text-red-300 ring-red-400/30",
    amber: "bg-amber-500/10 text-amber-200 ring-amber-400/30",
    sky: "bg-sky-500/10 text-sky-300 ring-sky-400/30",
    violet: "bg-violet-500/10 text-violet-300 ring-violet-400/30",
  };
  return (
    <span
      className={
        "inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium ring-1 " +
        tones[tone]
      }
    >
      {children}
    </span>
  );
};

// --- MAIN COMPONENTS ---
const SignalHeader = ({ payload }) => {
  const s = statusStyle(payload.signal_status);
  const Icon = s.icon;
  const ts = new Date(payload.timestamp);
  return (
    <div className="flex flex-wrap items-center justify-between gap-4">
      <div className="flex items-center gap-4">
        <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-gradient-to-br from-violet-600/40 to-sky-500/30 ring-1 ring-white/10">
          <Activity className="h-7 w-7 text-sky-300" />
        </div>
        <div>
          <div className="flex items-center gap-2 text-xs uppercase tracking-widest text-slate-400">
            <span>FITPAC Alert Engine</span>
            <span className="text-slate-600">•</span>
            <span>v4.7</span>
          </div>
          <div className="flex items-baseline gap-3">
            <h1 className="text-3xl font-black text-white">{payload.ticker}</h1>
            <span className="text-sm text-slate-400">
              {ts.toUTCString().replace("GMT", "UTC")}
            </span>
          </div>
        </div>
      </div>

      <div
        className={
          "flex items-center gap-3 rounded-2xl px-5 py-3 ring-1 " +
          s.bg +
          " " +
          s.ring
        }
      >
        <span className="relative flex h-3 w-3">
          <span
            className={
              "absolute inline-flex h-full w-full animate-ping rounded-full opacity-75 " +
              s.dot
            }
          />
          <span className={"relative inline-flex h-3 w-3 rounded-full " + s.dot} />
        </span>
        <Icon className={"h-5 w-5 " + s.text} />
        <span className={"text-sm font-bold tracking-wider " + s.text}>
          {s.label}
        </span>
      </div>
    </div>
  );
};

const VetoBanner = ({ warnings }) => (
  <div className="flex items-start gap-4 rounded-2xl border border-red-500/40 bg-gradient-to-r from-red-950/70 via-red-900/30 to-red-950/70 p-5 shadow-lg shadow-red-900/20">
    <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-red-500/20 ring-1 ring-red-400/40">
      <AlertTriangle className="h-5 w-5 text-red-300" />
    </div>
    <div className="flex-1">
      <div className="flex items-center gap-2">
        <span className="text-sm font-bold uppercase tracking-widest text-red-300">
          Hard Veto Active
        </span>
        <Pill tone="red">
          <ShieldAlert className="h-3 w-3" /> FITPAC Protocol Engaged
        </Pill>
      </div>
      <div className="mt-1 space-y-1">
        {warnings.map((w, i) => (
          <p key={i} className="text-sm text-red-100/90">
            {w}
          </p>
        ))}
      </div>
    </div>
  </div>
);

const ConfidenceGauge = ({ score, status }) => {
  const pct = Math.round(score * 100);
  const color =
    status === "VETOED"
      ? "#ef4444"
      : status === "VIRAL_IGNITION"
      ? "#10b981"
      : "#f59e0b";
  const data = [{ name: "conf", value: pct, fill: color }];
  return (
    <Card className="p-5">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-xs uppercase tracking-widest text-slate-400">
          <Target className="h-3.5 w-3.5" />
          <span>Pump Probability</span>
        </div>
        <Pill tone="slate">GBDT Ensemble</Pill>
      </div>
      <div className="relative mt-2 h-56">
        <ResponsiveContainer width="100%" height="100%">
          <RadialBarChart
            innerRadius="72%"
            outerRadius="100%"
            data={data}
            startAngle={220}
            endAngle={-40}
          >
            <PolarAngleAxis type="number" domain={[0, 100]} tick={false} />
            <RadialBar
              background={{ fill: "#1e293b" }}
              dataKey="value"
              cornerRadius={10}
            />
          </RadialBarChart>
        </ResponsiveContainer>
        <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-center">
          <div className="text-5xl font-black text-white tabular-nums">{pct}</div>
          <div className="text-xs uppercase tracking-widest text-slate-400">
            Confidence
          </div>
        </div>
      </div>
    </Card>
  );
};

const MetricBar = ({ label, value, icon: Icon, tone = "sky", subtitle }) => {
  const pct = Math.min(100, Math.max(0, value * 100));
  const toneMap = {
    sky: "from-sky-500 to-cyan-400",
    emerald: "from-emerald-500 to-teal-400",
    violet: "from-violet-500 to-fuchsia-400",
    amber: "from-amber-500 to-orange-400",
    red: "from-red-500 to-rose-400",
  };
  return (
    <div>
      <div className="mb-1.5 flex items-center justify-between">
        <div className="flex items-center gap-2 text-xs text-slate-400">
          <Icon className="h-3.5 w-3.5" />
          <span className="uppercase tracking-widest">{label}</span>
        </div>
        <div className="text-sm font-bold text-white tabular-nums">
          {fmtPct(value)}
        </div>
      </div>
      <div className="h-2 overflow-hidden rounded-full bg-slate-800/70 ring-1 ring-white/5">
        <div
          className={"h-full rounded-full bg-gradient-to-r " + toneMap[tone]}
          style={{ width: pct + "%" }}
        />
      </div>
      {subtitle && (
        <div className="mt-1 text-[11px] text-slate-500">{subtitle}</div>
      )}
    </div>
  );
};

const SocialCard = ({ social }) => {
  const authLow = social.authenticity_score < 0.3;
  return (
    <Card className="p-5">
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-sky-500/15 ring-1 ring-sky-400/30">
            <Zap className="h-4 w-4 text-sky-300" />
          </div>
          <h3 className="text-sm font-bold uppercase tracking-widest text-slate-200">
            Social Transformer
          </h3>
        </div>
        {authLow ? (
          <Pill tone="red">
            <Bot className="h-3 w-3" /> Bot Swarm
          </Pill>
        ) : (
          <Pill tone="emerald">
            <BadgeCheck className="h-3 w-3" /> Authentic
          </Pill>
        )}
      </div>

      <div className="space-y-4">
        <MetricBar
          label="Hype Velocity"
          value={social.hype_velocity}
          icon={TrendingUp}
          tone="violet"
          subtitle="Rate of viral mention acceleration"
        />
        <MetricBar
          label="Authenticity Score"
          value={social.authenticity_score}
          icon={ShieldCheck}
          tone={authLow ? "red" : "emerald"}
          subtitle="1.0 = fully organic • <0.3 = bot-filtered"
        />
        <div>
          <div className="mb-1.5 flex items-center justify-between">
            <div className="flex items-center gap-2 text-xs text-slate-400">
              <Flame className="h-3.5 w-3.5" />
              <span className="uppercase tracking-widest">
                Influence Weight
              </span>
            </div>
            <div className="text-sm font-bold text-white tabular-nums">
              ×{social.influence_weight.toFixed(2)}
            </div>
          </div>
          <div className="text-[11px] text-slate-500">
            VIP multiplier × dynamic fire score
          </div>
        </div>

        <div>
          <div className="mb-2 flex items-center gap-2 text-xs uppercase tracking-widest text-slate-400">
            <Eye className="h-3.5 w-3.5" />
            <span>VIP Triggers</span>
          </div>
          <div className="flex flex-wrap gap-2">
            {social.vip_triggers.length === 0 ? (
              <span className="text-xs text-slate-500">
                No VIP accounts detected in stream
              </span>
            ) : (
              social.vip_triggers.map((v) => (
                <span
                  key={v}
                  className="inline-flex items-center gap-1.5 rounded-full bg-gradient-to-r from-sky-500/20 to-violet-500/20 px-3 py-1 text-xs font-semibold text-white ring-1 ring-sky-400/40"
                >
                  <BadgeCheck className="h-3.5 w-3.5 text-sky-300" />
                  {v}
                </span>
              ))
            )}
          </div>
        </div>
      </div>
    </Card>
  );
};

const ChainCard = ({ chain }) => {
  const distPct = chain.insider_distribution_ratio;
  const distTone =
    distPct > 0.4 ? "red" : distPct > 0.25 ? "amber" : "emerald";
  const liqTone =
    chain.liquidity_depth_usd < 50_000
      ? "red"
      : chain.liquidity_depth_usd < 150_000
      ? "amber"
      : "emerald";

  return (
    <Card className="p-5">
      <div className="mb-4 flex items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-emerald-500/15 ring-1 ring-emerald-400/30">
            <Droplets className="h-4 w-4 text-emerald-300" />
          </div>
          <h3 className="text-sm font-bold uppercase tracking-widest text-slate-200">
            On-Chain Veto Engine
          </h3>
        </div>
        {chain.hard_veto_active ? (
          <Pill tone="red">
            <ShieldAlert className="h-3 w-3" /> Veto
          </Pill>
        ) : (
          <Pill tone="emerald">
            <ShieldCheck className="h-3 w-3" /> Clear
          </Pill>
        )}
      </div>

      <div className="space-y-4">
        <MetricBar
          label="Insider Distribution"
          value={chain.insider_distribution_ratio}
          icon={Skull}
          tone={distTone}
          subtitle="Veto triggered above 40%"
        />

        <div className="rounded-xl border border-white/5 bg-slate-950/50 p-4">
          <div className="flex items-center justify-between">
            <div>
              <div className="flex items-center gap-2 text-xs uppercase tracking-widest text-slate-400">
                <Droplets className="h-3.5 w-3.5" />
                <span>Liquidity Depth</span>
              </div>
              <div className="mt-1 text-2xl font-black text-white tabular-nums">
                {fmtUSD(chain.liquidity_depth_usd)}
              </div>
            </div>
            <Pill tone={liqTone}>
              {chain.liquidity_depth_usd < 50_000
                ? "Thin"
                : chain.liquidity_depth_usd < 150_000
                ? "Moderate"
                : "Deep"}
            </Pill>
          </div>
        </div>

        <div
          className={
            "rounded-xl border p-4 " +
            (chain.hard_veto_active
              ? "border-red-500/30 bg-red-950/30"
              : "border-emerald-500/20 bg-emerald-950/20")
          }
        >
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 text-xs uppercase tracking-widest text-slate-400">
              {chain.hard_veto_active ? (
                <ShieldAlert className="h-3.5 w-3.5 text-red-300" />
              ) : (
                <ShieldCheck className="h-3.5 w-3.5 text-emerald-300" />
              )}
              <span>Hard Veto State</span>
            </div>
            <span
              className={
                "text-sm font-bold " +
                (chain.hard_veto_active ? "text-red-300" : "text-emerald-300")
              }
            >
              {chain.hard_veto_active ? "ACTIVE" : "INACTIVE"}
            </span>
          </div>
        </div>
      </div>
    </Card>
  );
};

const VelocitySpark = ({ seed }) => {
  // Generate deterministic mock sparkline from seed
  const data = useMemo(() => {
    const rng = (i) =>
      (Math.sin(seed * 9.3 + i * 2.1) + 1) / 2 + 0.1 * Math.sin(i * 0.7);
    return Array.from({ length: 24 }, (_, i) => ({
      t: i,
      v: Math.max(0.02, Math.min(1, rng(i))),
    }));
  }, [seed]);

  return (
    <Card className="p-5">
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-center gap-2 text-xs uppercase tracking-widest text-slate-400">
          <TrendingUp className="h-3.5 w-3.5" />
          <span>Hype Velocity — Last 24 Ticks</span>
        </div>
        <Pill tone="violet">Streaming</Pill>
      </div>
      <div className="h-40">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={data}>
            <defs>
              <linearGradient id="velGrad" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#a78bfa" stopOpacity={0.7} />
                <stop offset="100%" stopColor="#a78bfa" stopOpacity={0.05} />
              </linearGradient>
            </defs>
            <XAxis dataKey="t" hide />
            <YAxis domain={[0, 1]} hide />
            <Tooltip
              contentStyle={{
                backgroundColor: "#0f172a",
                border: "1px solid #334155",
                borderRadius: "8px",
                fontSize: "12px",
              }}
              labelStyle={{ color: "#94a3b8" }}
              formatter={(v) => [fmtPct(v), "Velocity"]}
            />
            <Area
              type="monotone"
              dataKey="v"
              stroke="#a78bfa"
              strokeWidth={2}
              fill="url(#velGrad)"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
};

const AuditLog = ({ payload }) => {
  const lines = useMemo(() => {
    const out = [
      `[INFO] Running backend cycle for ${payload.ticker}...`,
      `[INFO] Social stream processed: hype_velocity=${payload.social_metrics.hype_velocity.toFixed(
        2
      )}, authenticity=${payload.social_metrics.authenticity_score.toFixed(2)}`,
      `[INFO] VIP triggers resolved: ${
        payload.social_metrics.vip_triggers.length
          ? payload.social_metrics.vip_triggers.join(", ")
          : "(none)"
      }`,
      `[INFO] Chain veto state: ${
        payload.chain_metrics.hard_veto_active ? "ACTIVE" : "INACTIVE"
      } (insider_dist=${fmtPct(
        payload.chain_metrics.insider_distribution_ratio
      )})`,
    ];
    if (payload.chain_metrics.hard_veto_active) {
      out.push(
        `[ERROR] Trap_Evaded: High social hype + insider distribution. VETOED.`
      );
    }
    out.push(
      `[INFO] Confidence emitted: ${payload.confidence_score.toFixed(
        3
      )} → signal_status=${payload.signal_status}`
    );
    return out;
  }, [payload]);

  return (
    <Card className="p-5">
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-center gap-2 text-xs uppercase tracking-widest text-slate-400">
          <Clock className="h-3.5 w-3.5" />
          <span>FITPAC Audit Log</span>
        </div>
        <Pill tone="slate">Compliance Trace</Pill>
      </div>
      <div className="overflow-hidden rounded-xl border border-white/5 bg-black/60 p-4 font-mono text-[12px] leading-relaxed">
        {lines.map((l, i) => {
          const isErr = l.includes("[ERROR]");
          return (
            <div
              key={i}
              className={
                "whitespace-pre-wrap " +
                (isErr ? "text-red-300" : "text-emerald-300/90")
              }
            >
              <span className="text-slate-500">FITPAC_EMIT:</span> {l}
            </div>
          );
        })}
      </div>
    </Card>
  );
};

const JsonViewer = ({ payload }) => (
  <Card className="p-5">
    <div className="mb-3 flex items-center justify-between">
      <div className="flex items-center gap-2 text-xs uppercase tracking-widest text-slate-400">
        <Activity className="h-3.5 w-3.5" />
        <span>Raw UI Payload</span>
      </div>
      <Pill tone="sky">backend → ui</Pill>
    </div>
    <pre className="max-h-72 overflow-auto rounded-xl border border-white/5 bg-black/60 p-4 font-mono text-[11px] leading-relaxed text-slate-300">
      {JSON.stringify(payload, null, 2)}
    </pre>
  </Card>
);

// --- ROOT DASHBOARD ---
export default function FitpacDashboard() {
  const [scenario, setScenario] = useState("ignition");
  const [tick, setTick] = useState(0);
  const payload = PAYLOADS[scenario];

  useEffect(() => {
    const id = setInterval(() => setTick((t) => t + 1), 4000);
    return () => clearInterval(id);
  }, []);

  const scenarios = [
    { id: "ignition", label: "Viral Ignition", tone: "emerald" },
    { id: "standby", label: "Standby", tone: "amber" },
    { id: "vetoed", label: "Vetoed Trap", tone: "red" },
  ];

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      {/* Ambient glow */}
      <div className="pointer-events-none fixed inset-0 overflow-hidden">
        <div className="absolute -top-40 -left-40 h-96 w-96 rounded-full bg-violet-600/20 blur-3xl" />
        <div className="absolute top-40 -right-40 h-96 w-96 rounded-full bg-sky-600/20 blur-3xl" />
        <div className="absolute bottom-0 left-1/2 h-96 w-96 -translate-x-1/2 rounded-full bg-emerald-600/10 blur-3xl" />
      </div>

      <div className="relative mx-auto max-w-7xl px-6 py-8">
        {/* Scenario switcher */}
        <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            {scenarios.map((s) => (
              <button
                key={s.id}
                onClick={() => setScenario(s.id)}
                className={
                  "rounded-full px-4 py-1.5 text-xs font-semibold ring-1 transition " +
                  (scenario === s.id
                    ? s.tone === "emerald"
                      ? "bg-emerald-500/20 text-emerald-200 ring-emerald-400/40"
                      : s.tone === "red"
                      ? "bg-red-500/20 text-red-200 ring-red-400/40"
                      : "bg-amber-500/20 text-amber-100 ring-amber-400/40"
                    : "bg-slate-800/50 text-slate-400 ring-slate-700 hover:text-slate-200")
                }
              >
                {s.label}
              </button>
            ))}
          </div>
          <button
            onClick={() => setTick((t) => t + 1)}
            className="inline-flex items-center gap-2 rounded-full bg-slate-800/70 px-4 py-1.5 text-xs font-semibold text-slate-200 ring-1 ring-slate-700 hover:bg-slate-700"
          >
            <RefreshCw className="h-3.5 w-3.5" />
            Refresh Cycle
          </button>
        </div>

        {/* Header */}
        <SignalHeader payload={payload} />

        {/* Veto banner */}
        {payload.chain_metrics.hard_veto_active && (
          <div className="mt-6">
            <VetoBanner warnings={payload.system_warnings} />
          </div>
        )}

        {/* Main grid */}
        <div className="mt-6 grid grid-cols-1 gap-6 lg:grid-cols-3">
          <div className="lg:col-span-1">
            <ConfidenceGauge
              score={payload.confidence_score}
              status={payload.signal_status}
            />
          </div>
          <div className="lg:col-span-2">
            <VelocitySpark seed={tick + (scenario === "ignition" ? 1 : scenario === "vetoed" ? 7 : 3)} />
          </div>

          <SocialCard social={payload.social_metrics} />
          <ChainCard chain={payload.chain_metrics} />
          <AuditLog payload={payload} />

          <div className="lg:col-span-3">
            <JsonViewer payload={payload} />
          </div>
        </div>

        <footer className="mt-10 flex flex-wrap items-center justify-between gap-3 text-[11px] uppercase tracking-widest text-slate-500">
          <span>FITPAC Alert Engine · UI Payload Consumer</span>
          <span>No Kelly · No Dynamic Sizing · Signal-Only Mode</span>
        </footer>
      </div>
    </div>
  );
}
