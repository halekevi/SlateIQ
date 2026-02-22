import React, { useEffect, useMemo, useState } from "react";

/**
 * PrizePicks Payout Calculator (Standard + Goblin/Demon)
 *
 * ✅ Standard payouts are fixed tables:
 * Power: 2=3x, 3=6x, 4=10x, 5=20x, 6=37.5x
 * Flex top: 3=3x, 4=6x, 5=10x, 6=25x + partial tiers
 *
 * ⚠️ Goblin/Demon multipliers vary on PrizePicks; “Estimate mode” uses per-leg heuristic modifiers.
 * ✅ “Exact override” lets you enter the slip multiplier shown in the app for perfect matches.
 */

// ----------------------
// Defaults (PrizePicks *Standard* payout tables)
// Source (official): PrizePicks payout help/resources pages.
// ----------------------
const DEFAULT_POWER_BASE = { 2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 37.5 };

const DEFAULT_FLEX_BASE = {
  2: { 2: 3.0 },
  3: { 3: 3.0, 2: 1.0 },
  4: { 4: 6.0, 3: 1.5 },
  5: { 5: 10.0, 4: 2.0, 3: 0.4 },
  6: { 6: 25.0, 5: 2.0, 4: 0.4 },
};

// ----------------------
// Heuristic modifiers (ESTIMATE MODE)
// (Keep these if they match your internal model; otherwise tweak)
// ----------------------
// Default heuristics (ESTIMATE MODE) — tweak these to match what you observe in-app.
const DEFAULT_GOBLIN_POWER = { 1: 0.84, 2: 0.747, 3: 0.707 };
const DEFAULT_GOBLIN_FLEX = { 1: 0.8, 2: 0.72, 3: 0.6 };

const DEFAULT_DEMON_POWER = { 1: 1.627, 2: 2.4, 3: 2.72 };
const DEFAULT_DEMON_FLEX = { 1: 1.6, 2: 1.52, 3: 1.56 };

const SETTINGS_KEY = "pp_payout_calc_settings_v2";

const LEG_TYPES = [
  "Standard",
  "Goblin -1",
  "Goblin -2",
  "Goblin -3",
  "Demon +1",
  "Demon +2",
  "Demon +3",
];

// ----------------------
// Helpers
// ----------------------
function round2(x) {
  const n = Number(x);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 100) / 100;
}

function money(x) {
  const n = Number(x);
  if (!Number.isFinite(n)) return "";
  return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function classify(legType) {
  const t = String(legType || "");
  if (t.startsWith("Goblin")) {
    const dev = parseInt(t.split("-")[1], 10);
    return { kind: "goblin", dev: Number.isFinite(dev) ? dev : 1 };
  }
  if (t.startsWith("Demon")) {
    const dev = parseInt(t.split("+")[1], 10);
    return { kind: "demon", dev: Number.isFinite(dev) ? dev : 1 };
  }
  return { kind: "standard", dev: 0 };
}

function legEmoji(t) {
  if (String(t).startsWith("Goblin")) return "👺";
  if (String(t).startsWith("Demon")) return "😈";
  return "⭐";
}

function legColor(t) {
  if (String(t).startsWith("Goblin")) return "#7c3aed";
  if (String(t).startsWith("Demon")) return "#dc2626";
  return "#2563eb";
}

function buildModifiers(legs, mods) {
  let pm = 1;
  let fm = 1;
  for (const leg of legs) {
    const { kind, dev } = classify(leg);
    if (kind === "goblin") {
      pm *= mods.goblinPower[dev] ?? mods.goblinPower[1];
      fm *= mods.goblinFlex[dev] ?? mods.goblinFlex[1];
    } else if (kind === "demon") {
      pm *= mods.demonPower[dev] ?? mods.demonPower[1];
      fm *= mods.demonFlex[dev] ?? mods.demonFlex[1];
    }
  }
  return { pm, fm };
}

function calcPayouts({
  legs,
  stake,
  mode, // "estimate" | "exact"
  exactPowerMult,
  exactFlexMult,
  tables,
  mods,
}) {
  const n = legs.length;
  if (n < 2) return null;

  const basePower = tables.powerBase[n];
  const baseFlex = tables.flexBase[n] || {};

  if (!basePower || Object.keys(baseFlex).length === 0) return null;

  // Estimate mode uses per-leg modifiers.
  // Exact mode uses user overrides if provided; if not, fallback to estimate.
  const { pm, fm } = buildModifiers(legs, mods);

  const estPowerMult = round2(basePower * pm);
  const estFlexMultTop = round2((baseFlex[n] ?? 0) * fm);

  const powerMult =
    mode === "exact" && Number.isFinite(exactPowerMult) && exactPowerMult > 0
      ? round2(exactPowerMult)
      : estPowerMult;

  const flexMultTop =
    mode === "exact" && Number.isFinite(exactFlexMult) && exactFlexMult > 0
      ? round2(exactFlexMult)
      : estFlexMultTop;

  // Flex partial tiers: apply *same* flex modifier in estimate mode;
  // in exact mode, we keep the base partials unless you want to override them too.
  // (PP partials can also change with gob/demon; if you observe that, we can add exact partial overrides.)
  const partials = Object.entries(baseFlex)
    .map(([k, mult]) => ({ correct: parseInt(k, 10), mult: Number(mult) }))
    .filter((x) => x.correct < n && Number.isFinite(x.correct) && Number.isFinite(x.mult))
    .sort((a, b) => b.correct - a.correct);

  const partialsAdj =
    mode === "estimate"
      ? partials.map((p) => ({ ...p, mult_adj: round2(p.mult * fm) }))
      : partials.map((p) => ({ ...p, mult_adj: round2(p.mult) }));

  const powerWin = round2(stake * powerMult);
  const flexWinTop = round2(stake * flexMultTop);

  // Two related but different concepts:
  // - stakeToWin100: how much you must stake to win $100 at this multiplier
  // - breakevenPct: probability needed to break even (ignoring pushes/partials)
  const stakeToWin100Power = powerMult > 0 ? round2(100 / powerMult) : 0;
  const stakeToWin100Flex = flexMultTop > 0 ? round2(100 / flexMultTop) : 0;
  const breakevenPctPower = powerMult > 0 ? round2((1 / powerMult) * 100) : 0;
  const breakevenPctFlex = flexMultTop > 0 ? round2((1 / flexMultTop) * 100) : 0;

  return {
    n,
    mode,
    stake,
    basePower,
    baseFlexTop: baseFlex[n] ?? 0,
    pm,
    fm,
    powerMult,
    flexMultTop,
    powerWin,
    flexWinTop,
    stakeToWin100Power,
    stakeToWin100Flex,
    breakevenPctPower,
    breakevenPctFlex,
    partials: partialsAdj.map((p) => ({
      correct: p.correct,
      mult: p.mult,
      mult_adj: p.mult_adj,
      win: round2(stake * p.mult_adj),
    })),
  };
}

// ----------------------
// Component
// ----------------------
export default function PayoutCalculator() {
  const [tab, setTab] = useState("builder"); // builder | reference
  const [stake, setStake] = useState(10);
  const [legs, setLegs] = useState(["Standard", "Standard", "Standard"]);

  // Settings
  const [showSettings, setShowSettings] = useState(false);
  const [powerBase, setPowerBase] = useState(DEFAULT_POWER_BASE);
  const [flexBase, setFlexBase] = useState(DEFAULT_FLEX_BASE);
  const [goblinPower, setGoblinPower] = useState(DEFAULT_GOBLIN_POWER);
  const [goblinFlex, setGoblinFlex] = useState(DEFAULT_GOBLIN_FLEX);
  const [demonPower, setDemonPower] = useState(DEFAULT_DEMON_POWER);
  const [demonFlex, setDemonFlex] = useState(DEFAULT_DEMON_FLEX);

  // Mode controls
  const [mode, setMode] = useState("estimate"); // estimate | exact
  const [exactPowerMult, setExactPowerMult] = useState("");
  const [exactFlexMult, setExactFlexMult] = useState("");

  // Reference tab
  const [refLegs, setRefLegs] = useState(4);

  // Load persisted settings
  useEffect(() => {
    try {
      const raw = localStorage.getItem(SETTINGS_KEY);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (parsed?.powerBase) setPowerBase(parsed.powerBase);
      if (parsed?.flexBase) setFlexBase(parsed.flexBase);
      if (parsed?.goblinPower) setGoblinPower(parsed.goblinPower);
      if (parsed?.goblinFlex) setGoblinFlex(parsed.goblinFlex);
      if (parsed?.demonPower) setDemonPower(parsed.demonPower);
      if (parsed?.demonFlex) setDemonFlex(parsed.demonFlex);
    } catch {
      // ignore
    }
  }, []);

  // Persist settings
  useEffect(() => {
    try {
      localStorage.setItem(
        SETTINGS_KEY,
        JSON.stringify({ powerBase, flexBase, goblinPower, goblinFlex, demonPower, demonFlex })
      );
    } catch {
      // ignore
    }
  }, [powerBase, flexBase, goblinPower, goblinFlex, demonPower, demonFlex]);

  const parsedExactPower = useMemo(() => {
    const n = parseFloat(exactPowerMult);
    return Number.isFinite(n) ? n : NaN;
  }, [exactPowerMult]);

  const parsedExactFlex = useMemo(() => {
    const n = parseFloat(exactFlexMult);
    return Number.isFinite(n) ? n : NaN;
  }, [exactFlexMult]);

  const result = useMemo(
    () =>
      calcPayouts({
        legs,
        stake: Math.max(1, Number(stake) || 10),
        mode,
        exactPowerMult: parsedExactPower,
        exactFlexMult: parsedExactFlex,
        tables: { powerBase, flexBase },
        mods: { goblinPower, goblinFlex, demonPower, demonFlex },
      }),
    [legs, stake, mode, parsedExactPower, parsedExactFlex, powerBase, flexBase, goblinPower, goblinFlex, demonPower, demonFlex]
  );

  function addLeg() {
    if (legs.length >= 6) return;
    setLegs((prev) => [...prev, "Standard"]);
  }

  function removeLeg(i) {
    if (legs.length <= 2) return;
    setLegs((prev) => prev.filter((_, idx) => idx !== i));
  }

  function setLegType(i, value) {
    setLegs((prev) => prev.map((x, idx) => (idx === i ? value : x)));
  }

  const badgeSummary = useMemo(() => {
    const counts = { Standard: 0, Goblin: 0, Demon: 0 };
    for (const l of legs) {
      const { kind } = classify(l);
      if (kind === "goblin") counts.Goblin += 1;
      else if (kind === "demon") counts.Demon += 1;
      else counts.Standard += 1;
    }
    return counts;
  }, [legs]);

  // Styles (simple, self-contained)
  const styles = {
    page: { fontFamily: "system-ui, Segoe UI, Roboto, Arial", background: "#030712", color: "#f1f5f9", minHeight: "100vh" },
    header: { padding: "18px 22px", borderBottom: "1px solid #1e293b", background: "#0a0f1e", display: "flex", justifyContent: "space-between", alignItems: "center", flexWrap: "wrap", gap: 12 },
    title: { fontWeight: 800, letterSpacing: 1, fontSize: 18 },
    sub: { fontSize: 11, color: "#64748b", marginTop: 2, letterSpacing: 2 },
    tabs: { display: "flex", gap: 6, padding: "10px 22px", borderBottom: "1px solid #1e293b", background: "#070d1a" },
    tabBtn: (active) => ({
      border: "none",
      cursor: "pointer",
      padding: "10px 14px",
      borderRadius: 10,
      background: active ? "rgba(59,130,246,0.16)" : "transparent",
      color: active ? "#f8fafc" : "#94a3b8",
      fontWeight: 800,
      letterSpacing: 1,
    }),
    wrap: { maxWidth: 980, margin: "0 auto", padding: "18px 16px" },
    grid: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 18 },
    card: { border: "1px solid #1e293b", background: "rgba(255,255,255,0.03)", borderRadius: 14, padding: 14 },
    label: { fontSize: 11, color: "#94a3b8", letterSpacing: 2, marginBottom: 10, fontWeight: 800 },
    legRow: { display: "flex", alignItems: "center", gap: 8, padding: "8px 10px", border: "1px solid #1e293b", borderRadius: 12, marginBottom: 8, background: "rgba(255,255,255,0.02)" },
    select: { flex: 1, background: "#0f172a", color: "#f8fafc", border: "1px solid #334155", borderRadius: 10, padding: "8px 10px", fontWeight: 800 },
    btn: { border: "1px solid #334155", background: "rgba(59,130,246,0.10)", color: "#60a5fa", borderRadius: 12, padding: "10px 12px", cursor: "pointer", fontWeight: 900, letterSpacing: 1 },
    dangerBtn: { border: "none", background: "rgba(239,68,68,0.14)", color: "#ef4444", borderRadius: 10, padding: "6px 10px", cursor: "pointer", fontWeight: 900 },
    pill: (bg) => ({ background: bg, color: "#0b1220", borderRadius: 999, padding: "6px 10px", fontWeight: 900, fontSize: 12 }),
    input: { width: 110, background: "#0f172a", color: "#f8fafc", border: "1px solid #334155", borderRadius: 10, padding: "8px 10px", fontWeight: 800 },
    inputSm: { width: 86, background: "#0f172a", color: "#f8fafc", border: "1px solid #334155", borderRadius: 10, padding: "6px 8px", fontWeight: 800 },
    small: { fontSize: 12, color: "#94a3b8" },
    resultRow: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 },
    metric: { border: "1px solid #1e293b", borderRadius: 12, background: "rgba(255,255,255,0.02)", padding: 12, textAlign: "center" },
    metricLabel: { fontSize: 10, color: "#94a3b8", letterSpacing: 2, fontWeight: 900 },
    metricVal: { fontSize: 22, fontWeight: 900, marginTop: 4 },
    table: { width: "100%", borderCollapse: "collapse" },
    th: { textAlign: "center", padding: "10px 10px", fontSize: 11, letterSpacing: 2, color: "#94a3b8", borderBottom: "1px solid #1e293b" },
    td: { textAlign: "center", padding: "10px 10px", fontSize: 13, borderBottom: "1px solid #0f172a" },
  };

  const canAdd = legs.length < 6;
  const canRemove = legs.length > 2;

  function resetSettings() {
    setPowerBase(DEFAULT_POWER_BASE);
    setFlexBase(DEFAULT_FLEX_BASE);
    setGoblinPower(DEFAULT_GOBLIN_POWER);
    setGoblinFlex(DEFAULT_GOBLIN_FLEX);
    setDemonPower(DEFAULT_DEMON_POWER);
    setDemonFlex(DEFAULT_DEMON_FLEX);
  }

  function updatePowerBase(n, val) {
    const x = parseFloat(val);
    setPowerBase((prev) => ({ ...prev, [n]: Number.isFinite(x) ? x : prev[n] }));
  }

  function updateFlexBase(n, correct, val) {
    const x = parseFloat(val);
    setFlexBase((prev) => {
      const row = { ...(prev[n] || {}) };
      row[correct] = Number.isFinite(x) ? x : row[correct];
      return { ...prev, [n]: row };
    });
  }

  function updateMod(which, dev, val) {
    const x = parseFloat(val);
    const setter = {
      goblinPower: setGoblinPower,
      goblinFlex: setGoblinFlex,
      demonPower: setDemonPower,
      demonFlex: setDemonFlex,
    }[which];
    if (!setter) return;
    setter((prev) => ({ ...prev, [dev]: Number.isFinite(x) ? x : prev[dev] }));
  }

  return (
    <div style={styles.page}>
      <div style={styles.header}>
        <div>
          <div style={styles.title}>🎯 PRIZEPICKS PAYOUT CALCULATOR</div>
          <div style={styles.sub}>POWER · FLEX · STANDARD · GOBLIN · DEMON</div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10, flexWrap: "wrap" }}>
          <div style={styles.small}>STAKE $</div>
          <input
            type="number"
            min={1}
            value={stake}
            onChange={(e) => setStake(e.target.value)}
            style={styles.input}
          />
        </div>
      </div>

      <div style={styles.tabs}>
        <button style={styles.tabBtn(tab === "builder")} onClick={() => setTab("builder")}>
          🏗 BUILDER
        </button>
        <button style={styles.tabBtn(tab === "reference")} onClick={() => setTab("reference")}>
          📊 REFERENCE
        </button>
      </div>

      <div style={styles.wrap}>
        {tab === "builder" ? (
          <div style={styles.grid}>
            {/* LEFT */}
            <div style={styles.card}>
              <div style={styles.label}>TICKET · {legs.length} LEGS</div>

              {/* Settings */}
              <div style={{ marginBottom: 10 }}>
                <button
                  style={styles.tabBtn(showSettings)}
                  onClick={() => setShowSettings((s) => !s)}
                >
                  ⚙️ Settings
                </button>
                {showSettings && (
                  <div
                    style={{
                      marginTop: 10,
                      border: "1px solid #1e293b",
                      borderRadius: 12,
                      padding: 12,
                      background: "rgba(255,255,255,0.02)",
                    }}
                  >
                    <div
                      style={{
                        display: "flex",
                        justifyContent: "space-between",
                        alignItems: "center",
                        gap: 10,
                        flexWrap: "wrap",
                      }}
                    >
                      <div style={{ ...styles.small }}>
                        Update tables/modifiers here (auto-saves in browser).
                      </div>
                      <button
                        style={{
                          ...styles.dangerBtn,
                          background: "rgba(148,163,184,0.12)",
                          color: "#e2e8f0",
                        }}
                        onClick={resetSettings}
                      >
                        Reset defaults
                      </button>
                    </div>

                    <div style={{ marginTop: 10 }}>
                      <div style={{ ...styles.metricLabel, textAlign: "left", marginBottom: 8 }}>
                        STANDARD POWER MULTIPLIERS
                      </div>
                      <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                        {[2, 3, 4, 5, 6].map((n) => (
                          <div key={n} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                            <div style={{ width: 42, ...styles.small }}>{n}-leg</div>
                            <input
                              style={styles.inputSm}
                              value={powerBase[n]}
                              onChange={(e) => updatePowerBase(n, e.target.value)}
                            />
                          </div>
                        ))}
                      </div>
                    </div>

                    <div style={{ marginTop: 14 }}>
                      <div style={{ ...styles.metricLabel, textAlign: "left", marginBottom: 8 }}>
                        STANDARD FLEX TABLE (TOP + PARTIALS)
                      </div>
                      {[2, 3, 4, 5, 6].map((n) => (
                        <div key={n} style={{ marginBottom: 10 }}>
                          <div style={{ ...styles.small, marginBottom: 6 }}>{n}-leg flex</div>
                          <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                            {Object.keys(flexBase[n] || {})
                              .map((k) => parseInt(k, 10))
                              .sort((a, b) => b - a)
                              .map((correct) => (
                                <div key={correct} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                                  <div style={{ width: 60, ...styles.small }}>{correct}/{n}</div>
                                  <input
                                    style={styles.inputSm}
                                    value={flexBase[n]?.[correct] ?? ""}
                                    onChange={(e) => updateFlexBase(n, correct, e.target.value)}
                                  />
                                </div>
                              ))}
                          </div>
                        </div>
                      ))}
                    </div>

                    <div style={{ marginTop: 14 }}>
                      <div style={{ ...styles.metricLabel, textAlign: "left", marginBottom: 8 }}>
                        ESTIMATE MODIFIERS (PER LEG)
                      </div>
                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                        {[
                          ["Goblin Power", "goblinPower", goblinPower],
                          ["Goblin Flex", "goblinFlex", goblinFlex],
                          ["Demon Power", "demonPower", demonPower],
                          ["Demon Flex", "demonFlex", demonFlex],
                        ].map(([label, key, obj]) => (
                          <div
                            key={key}
                            style={{
                              border: "1px solid #0f172a",
                              borderRadius: 12,
                              padding: 10,
                              background: "rgba(255,255,255,0.02)",
                            }}
                          >
                            <div style={{ ...styles.small, marginBottom: 8, fontWeight: 900 }}>{label}</div>
                            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                              {[1, 2, 3].map((dev) => (
                                <div key={dev} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                                  <div style={{ width: 40, ...styles.small }}>{dev}</div>
                                  <input
                                    style={styles.inputSm}
                                    value={obj[dev]}
                                    onChange={(e) => updateMod(key, dev, e.target.value)}
                                  />
                                </div>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                      <div style={{ marginTop: 10, ...styles.small }}>
                        Tip: If you have the exact multiplier from PrizePicks for a specific mixed slip, use “Exact Override” instead of tuning these.
                      </div>
                    </div>
                  </div>
                )}
              </div>

              {/* Mode */}
              <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap", marginBottom: 10 }}>
                <div style={styles.small}>MODE:</div>
                <button
                  style={styles.tabBtn(mode === "estimate")}
                  onClick={() => setMode("estimate")}
                >
                  Estimate
                </button>
                <button
                  style={styles.tabBtn(mode === "exact")}
                  onClick={() => setMode("exact")}
                >
                  Exact Override
                </button>
              </div>

              {mode === "exact" && (
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 12 }}>
                  <div>
                    <div style={{ ...styles.small, marginBottom: 6 }}>Power Mult (from slip)</div>
                    <input
                      style={styles.input}
                      placeholder="e.g. 45"
                      value={exactPowerMult}
                      onChange={(e) => setExactPowerMult(e.target.value)}
                    />
                  </div>
                  <div>
                    <div style={{ ...styles.small, marginBottom: 6 }}>Flex Mult (top)</div>
                    <input
                      style={styles.input}
                      placeholder="e.g. 25"
                      value={exactFlexMult}
                      onChange={(e) => setExactFlexMult(e.target.value)}
                    />
                  </div>
                  <div style={{ gridColumn: "1 / -1", ...styles.small }}>
                    Tip: Paste the multipliers shown on PrizePicks for Goblin/Demon slips to match exactly.
                  </div>
                </div>
              )}

              {legs.map((t, i) => (
                <div key={i} style={styles.legRow}>
                  <div style={{ width: 24, textAlign: "center" }}>{legEmoji(t)}</div>
                  <div style={{ width: 70, fontSize: 12, color: "#94a3b8", fontWeight: 900 }}>
                    Leg {i + 1}
                  </div>
                  <select value={t} onChange={(e) => setLegType(i, e.target.value)} style={styles.select}>
                    {LEG_TYPES.map((opt) => (
                      <option key={opt} value={opt}>
                        {opt}
                      </option>
                    ))}
                  </select>
                  <button
                    style={{ ...styles.dangerBtn, opacity: canRemove ? 1 : 0.5 }}
                    onClick={() => canRemove && removeLeg(i)}
                    title={canRemove ? "Remove leg" : "Need at least 2 legs"}
                  >
                    ✕
                  </button>
                </div>
              ))}

              <button
                style={{ ...styles.btn, width: "100%", opacity: canAdd ? 1 : 0.5, marginTop: 8 }}
                onClick={() => canAdd && addLeg()}
              >
                + ADD LEG
              </button>

              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", marginTop: 12 }}>
                <span style={styles.pill("#e2e8f0")}>⭐ Standard: {badgeSummary.Standard}</span>
                <span style={styles.pill("#ddd6fe")}>👺 Goblin: {badgeSummary.Goblin}</span>
                <span style={styles.pill("#fee2e2")}>😈 Demon: {badgeSummary.Demon}</span>
              </div>
            </div>

            {/* RIGHT */}
            <div style={styles.card}>
              <div style={styles.label}>RESULTS</div>

              {!result ? (
                <div style={{ color: "#94a3b8", padding: "40px 0", textAlign: "center" }}>
                  Add at least 2 legs to see payouts.
                </div>
              ) : (
                <>
                  <div style={styles.resultRow}>
                    <div style={{ ...styles.card, borderColor: "#2563eb33", background: "linear-gradient(135deg,#0c1a3a,#071020)" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                        <div style={{ fontWeight: 900, letterSpacing: 2 }}>POWER</div>
                        <div style={{ color: "#94a3b8", fontSize: 12 }}>
                          {result.n}-leg · {result.powerMult}x
                        </div>
                      </div>

                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginTop: 12 }}>
                        <div style={styles.metric}>
                          <div style={styles.metricLabel}>WIN (ALL HIT)</div>
                          <div style={styles.metricVal}>${money(result.powerWin)}</div>
                        </div>
                        <div style={styles.metric}>
                          <div style={styles.metricLabel}>BREAKEVEN</div>
                          <div style={styles.metricVal}>{money(result.breakevenPctPower)}%</div>
                          <div style={{ ...styles.small, marginTop: 4 }}>
                            Stake to win $100: ${money(result.stakeToWin100Power)}
                          </div>
                        </div>
                      </div>

                      <div style={{ marginTop: 10, ...styles.small }}>
                        Base: {result.basePower}x{" "}
                        {mode === "estimate" ? (
                          <>
                            · Mod: {round2(result.pm)}x (est)
                          </>
                        ) : (
                          <>
                            · Override{" "}
                            {Number.isFinite(parsedExactPower) && parsedExactPower > 0 ? "(used)" : "(blank → est)"}
                          </>
                        )}
                      </div>
                    </div>

                    <div style={{ ...styles.card, borderColor: "#7c3aed33", background: "linear-gradient(135deg,#1a0a3a,#0f0620)" }}>
                      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
                        <div style={{ fontWeight: 900, letterSpacing: 2 }}>FLEX</div>
                        <div style={{ color: "#94a3b8", fontSize: 12 }}>
                          {result.n}-leg · top {result.flexMultTop}x
                        </div>
                      </div>

                      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginTop: 12 }}>
                        <div style={styles.metric}>
                          <div style={styles.metricLabel}>WIN (ALL HIT)</div>
                          <div style={styles.metricVal}>${money(result.flexWinTop)}</div>
                        </div>
                        <div style={styles.metric}>
                          <div style={styles.metricLabel}>BREAKEVEN</div>
                          <div style={styles.metricVal}>{money(result.breakevenPctFlex)}%</div>
                          <div style={{ ...styles.small, marginTop: 4 }}>
                            Stake to win $100: ${money(result.stakeToWin100Flex)}
                          </div>
                        </div>
                      </div>

                      <div style={{ marginTop: 10, ...styles.small }}>
                        Base top: {result.baseFlexTop}x{" "}
                        {mode === "estimate" ? (
                          <>
                            · Mod: {round2(result.fm)}x (est)
                          </>
                        ) : (
                          <>
                            · Override{" "}
                            {Number.isFinite(parsedExactFlex) && parsedExactFlex > 0 ? "(used)" : "(blank → est)"}
                          </>
                        )}
                      </div>

                      {result.partials.length > 0 && (
                        <div style={{ marginTop: 12, paddingTop: 10, borderTop: "1px solid rgba(124,58,237,0.2)" }}>
                          <div style={{ ...styles.metricLabel, textAlign: "left", marginBottom: 8 }}>
                            FLEX PARTIAL TIERS
                          </div>
                          <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                            {result.partials.map((p) => (
                              <div
                                key={p.correct}
                                style={{
                                  border: "1px solid rgba(124,58,237,0.25)",
                                  background: "rgba(124,58,237,0.10)",
                                  borderRadius: 12,
                                  padding: "10px 12px",
                                  minWidth: 140,
                                }}
                              >
                                <div style={{ fontSize: 11, color: "#a78bfa", fontWeight: 900 }}>
                                  {p.correct}/{result.n} correct
                                </div>
                                <div style={{ fontSize: 18, fontWeight: 900, marginTop: 3 }}>
                                  {p.mult_adj}x
                                </div>
                                <div style={{ fontSize: 11, color: "#10b981", fontWeight: 800, marginTop: 4 }}>
                                  ${money(p.win)}
                                </div>
                                {mode === "estimate" && (
                                  <div style={{ fontSize: 10, color: "#94a3b8", marginTop: 4 }}>
                                    (base {p.mult}x × est mod)
                                  </div>
                                )}
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>

                  <div style={{ marginTop: 12, ...styles.small }}>
                    Note: Standard tables are fixed. Goblin/Demon multipliers can vary on PrizePicks — use “Exact Override”
                    when you want a perfect match to the app.
                  </div>
                </>
              )}
            </div>
          </div>
        ) : (
          // ------------------
          // REFERENCE TAB
          // ------------------
          <div style={styles.card}>
            <div style={styles.label}>REFERENCE · STANDARD PAYOUTS</div>

            <div style={{ display: "flex", gap: 10, flexWrap: "wrap", alignItems: "center", marginBottom: 12 }}>
              <div style={styles.small}>LEG COUNT:</div>
              {[2, 3, 4, 5, 6].map((n) => (
                <button
                  key={n}
                  style={styles.tabBtn(refLegs === n)}
                  onClick={() => setRefLegs(n)}
                >
                  {n}-LEG
                </button>
              ))}
            </div>

            <div style={{ overflowX: "auto" }}>
              <table style={styles.table}>
                <thead>
                  <tr>
                    <th style={styles.th}>TYPE</th>
                    <th style={styles.th}>POWER</th>
                    <th style={styles.th}>WIN ${money(stake)} (POWER)</th>
                    <th style={styles.th}>$ TO WIN $100 (POWER)</th>
                    <th style={styles.th}>FLEX (TOP)</th>
                    <th style={styles.th}>WIN ${money(stake)} (FLEX TOP)</th>
                  </tr>
                </thead>
                <tbody>
                  {(() => {
                    const p = powerBase[refLegs];
                    const fTop = flexBase[refLegs]?.[refLegs] ?? 0;
                    const powerWin = round2((Number(stake) || 10) * p);
                    const flexWin = round2((Number(stake) || 10) * fTop);
                    const costP = p > 0 ? round2(100 / p) : 0;
                    const costF = fTop > 0 ? round2(100 / fTop) : 0;

                    return (
                      <tr>
                        <td style={styles.td}>STANDARD</td>
                        <td style={styles.td}>{p}x</td>
                        <td style={styles.td}>${money(powerWin)}</td>
                        <td style={styles.td}>${money(costP)}</td>
                        <td style={styles.td}>{fTop}x</td>
                        <td style={styles.td}>${money(flexWin)}</td>
                      </tr>
                    );
                  })()}
                </tbody>
              </table>
            </div>

            <div style={{ marginTop: 14 }}>
              <div style={{ ...styles.label, marginBottom: 10 }}>FLEX PARTIAL TIERS (STANDARD)</div>
              <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                {Object.entries(flexBase[refLegs] || {})
                  .map(([k, v]) => ({ correct: parseInt(k, 10), mult: Number(v) }))
                  .filter((x) => x.correct < refLegs)
                  .sort((a, b) => b.correct - a.correct)
                  .map((p) => (
                    <div
                      key={p.correct}
                      style={{
                        border: "1px solid #1e293b",
                        background: "rgba(255,255,255,0.02)",
                        borderRadius: 12,
                        padding: "10px 12px",
                        minWidth: 160,
                      }}
                    >
                      <div style={{ fontSize: 11, color: "#94a3b8", fontWeight: 900 }}>
                        {p.correct}/{refLegs} correct
                      </div>
                      <div style={{ fontSize: 18, fontWeight: 900, marginTop: 4 }}>{p.mult}x</div>
                      <div style={{ fontSize: 11, color: "#10b981", fontWeight: 800, marginTop: 6 }}>
                        ${money(round2((Number(stake) || 10) * p.mult))}
                      </div>
                    </div>
                  ))}
              </div>
            </div>

            <div style={{ marginTop: 14, ...styles.small }}>
              If you want Goblin/Demon *exact*, use the Builder tab → “Exact Override” and paste the slip multipliers from PrizePicks.
            </div>
          </div>
        )}
      </div>
    </div>
  );
}