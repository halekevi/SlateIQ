import { useState, useMemo, useEffect } from "react";

// ── Data ────────────────────────────────────────────────────────────────────
const DEFAULT_POWER_BASE = { 2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 37.5 };
const DEFAULT_FLEX_BASE = {
  2: { 2: 3.0 },
  3: { 3: 3.0, 2: 1.0 },
  4: { 4: 6.0, 3: 1.5 },
  5: { 5: 10.0, 4: 2.0, 3: 0.4 },
  6: { 6: 25.0, 5: 2.0, 4: 0.4 },
};
const DEFAULT_GOBLIN_POWER = { 1: 0.84, 2: 0.747, 3: 0.707 };
const DEFAULT_GOBLIN_FLEX  = { 1: 0.8,  2: 0.72,  3: 0.6 };
const DEFAULT_DEMON_POWER  = { 1: 1.627, 2: 2.4,  3: 2.72 };
const DEFAULT_DEMON_FLEX   = { 1: 1.6,  2: 1.52,  3: 1.56 };
const SETTINGS_KEY = "pp_payout_calc_settings_v2";
const LEG_TYPES = ["Standard","Goblin -1","Goblin -2","Goblin -3","Demon +1","Demon +2","Demon +3"];

// ── Helpers ─────────────────────────────────────────────────────────────────
const r2 = x => Math.round(Number(x) * 100) / 100;
const money = x => Number(x).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });

function classify(t) {
  if (t.startsWith("Goblin")) return { kind: "goblin", dev: parseInt(t.split("-")[1], 10) };
  if (t.startsWith("Demon"))  return { kind: "demon",  dev: parseInt(t.split("+")[1], 10) };
  return { kind: "standard", dev: 0 };
}
function legEmoji(t) {
  if (t.startsWith("Goblin")) return "👺";
  if (t.startsWith("Demon"))  return "😈";
  return "⭐";
}
function legColor(t) {
  if (t.startsWith("Goblin")) return "#a855f7";
  if (t.startsWith("Demon"))  return "#ef4444";
  return "#3b82f6";
}

function calcPayouts({ legs, stake, mode, exactPower, exactFlex, tables, mods }) {
  const n = legs.length;
  if (n < 2) return null;
  let pm = 1, fm = 1;
  for (const leg of legs) {
    const { kind, dev } = classify(leg);
    if (kind === "goblin") { pm *= mods.goblinPower[dev] ?? mods.goblinPower[1]; fm *= mods.goblinFlex[dev] ?? mods.goblinFlex[1]; }
    if (kind === "demon")  { pm *= mods.demonPower[dev]  ?? mods.demonPower[1];  fm *= mods.demonFlex[dev]  ?? mods.demonFlex[1]; }
  }
  const basePower = tables.powerBase[n] ?? 37.5;
  const baseFlex  = tables.flexBase[n]  ?? {};
  const estPM = r2(basePower * pm);
  const estFM = r2((baseFlex[n] ?? 25) * fm);
  const powerMult  = (mode === "exact" && exactPower > 0) ? r2(exactPower) : estPM;
  const flexMult   = (mode === "exact" && exactFlex  > 0) ? r2(exactFlex)  : estFM;
  const partials = Object.entries(baseFlex)
    .map(([k, v]) => ({ correct: +k, base: +v, adj: mode === "estimate" ? r2(+v * fm) : r2(+v) }))
    .filter(p => p.correct < n)
    .sort((a, b) => b.correct - a.correct);
  return {
    n, pm, fm, basePower, baseFlexTop: baseFlex[n] ?? 0,
    powerMult, flexMult,
    powerWin: r2(stake * powerMult),
    flexWin:  r2(stake * flexMult),
    breakeven: powerMult > 0 ? r2(100 / powerMult) : 0,
    partials: partials.map(p => ({ ...p, win: r2(stake * p.adj) })),
  };
}

// ── Styles ───────────────────────────────────────────────────────────────────
const C = {
  bg: "#09090b", surface: "#111114", surface2: "#18181c", surface3: "#1e1e23",
  border: "#27272a", border2: "#3f3f46",
  text: "#fafafa", muted: "#71717a", dim: "#3f3f46",
  green: "#22c55e", blue: "#3b82f6", purple: "#a855f7",
  gold: "#f59e0b", red: "#ef4444",
};

const mono = "'DM Mono', monospace";
const syne = "'Syne', sans-serif";

const S = {
  page:     { minHeight: "100vh", background: C.bg, color: C.text, fontFamily: "'Inter', sans-serif" },
  header:   { background: C.surface, borderBottom: `1px solid ${C.border}`, padding: "16px 28px", display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: 16, position: "sticky", top: 0, zIndex: 50 },
  logo:     { display: "flex", alignItems: "center", gap: 12 },
  logoIcon: { width: 38, height: 38, background: "linear-gradient(135deg,#f59e0b,#ef4444)", borderRadius: 10, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 20 },
  logoH1:   { fontFamily: syne, fontSize: 17, fontWeight: 800 },
  logoSub:  { fontFamily: mono, fontSize: 10, color: C.muted, letterSpacing: "1.5px", marginTop: 1 },
  tabs:     { background: C.surface2, borderBottom: `1px solid ${C.border}`, padding: "0 28px", display: "flex", gap: 2 },
  tabBtn:   (a) => ({ background: "none", border: "none", borderBottom: a ? `2px solid ${C.blue}` : "2px solid transparent", color: a ? C.text : C.muted, padding: "13px 18px", cursor: "pointer", fontFamily: mono, fontSize: 11, fontWeight: 500, letterSpacing: "1.5px", transition: "all .15s" }),
  wrap:     { maxWidth: 980, margin: "0 auto", padding: "28px 20px" },
  grid2:    { display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 },
  secLabel: { fontFamily: mono, fontSize: 10, color: C.muted, letterSpacing: "2.5px", marginBottom: 14, textTransform: "uppercase" },
  legRow:   (color) => ({ display: "flex", alignItems: "center", gap: 8, marginBottom: 8, background: C.surface2, borderRadius: 10, padding: "9px 12px", border: `1px solid ${color}28`, transition: "border-color .2s" }),
  legNum:   { fontFamily: mono, color: C.muted, fontSize: 11, width: 44, flexShrink: 0 },
  legSel:   (color) => ({ flex: 1, background: C.bg, border: `1px solid ${C.border2}`, borderRadius: 7, padding: "6px 10px", fontSize: 13, cursor: "pointer", outline: "none", fontFamily: mono, fontWeight: 500, color }),
  rmBtn:    { background: "rgba(239,68,68,.10)", color: C.red, border: "1px solid rgba(239,68,68,.18)", borderRadius: 7, padding: "5px 10px", cursor: "pointer", fontSize: 14, fontWeight: 700 },
  addBtn:   { width: "100%", padding: 10, background: "rgba(59,130,246,.05)", border: "1px dashed rgba(59,130,246,.30)", borderRadius: 10, color: C.blue, cursor: "pointer", fontFamily: mono, fontSize: 11, fontWeight: 500, letterSpacing: ".5px", marginTop: 4 },
  badge:    (bg, border, color) => ({ background: bg, border: `1px solid ${border}`, color, borderRadius: 99, padding: "4px 12px", fontFamily: mono, fontSize: 11, fontWeight: 500 }),
  card:     (c1, c2, bc) => ({ background: `linear-gradient(135deg,${c1},${c2})`, border: `1px solid ${bc}`, borderRadius: 14, padding: 18, marginBottom: 14 }),
  cardHead: { display: "flex", alignItems: "center", gap: 10, marginBottom: 16 },
  tag:      (bg, color, border) => ({ background: bg, color, border: `1px solid ${border}`, borderRadius: 6, padding: "3px 10px", fontFamily: mono, fontSize: 10, fontWeight: 500, letterSpacing: "1.5px" }),
  cardSub:  { color: C.muted, fontSize: 12 },
  metBox:   { background: "rgba(255,255,255,.03)", border: `1px solid ${C.border}`, borderRadius: 10, padding: 12, textAlign: "center" },
  metLabel: { fontFamily: mono, color: C.muted, fontSize: 9, letterSpacing: "2px", marginBottom: 6 },
  metVal:   (color, size = 24) => ({ fontFamily: syne, fontSize: size, fontWeight: 700, color }),
  metSub:   { fontFamily: mono, fontSize: 10, color: C.muted, marginTop: 4 },
  partWrap: { borderTop: "1px solid rgba(168,85,247,.15)", paddingTop: 12, marginTop: 14 },
  partBox:  { background: "rgba(168,85,247,.08)", border: "1px solid rgba(168,85,247,.20)", borderRadius: 10, padding: "10px 14px", textAlign: "center", minWidth: 80 },
  modBar:   { marginTop: 12, background: "rgba(255,255,255,.02)", borderRadius: 8, padding: "8px 12px", border: `1px solid ${C.border}` },
  input:    { background: C.surface2, color: C.text, border: `1px solid ${C.border2}`, borderRadius: 8, padding: "8px 12px", fontFamily: mono, fontSize: 14, fontWeight: 500, outline: "none", width: 100 },
  smallBtn: (a) => ({ background: a ? C.blue : C.surface2, color: a ? "#fff" : C.muted, border: `1px solid ${a ? C.blue : C.border}`, borderRadius: 7, padding: "6px 14px", cursor: "pointer", fontFamily: mono, fontSize: 11, fontWeight: 500, transition: "all .15s" }),
  table:    { width: "100%", borderCollapse: "collapse" },
  th:       { padding: "11px 14px", textAlign: "center", fontFamily: mono, color: C.muted, borderBottom: `1px solid ${C.border}`, fontSize: 10, letterSpacing: "1.5px", whiteSpace: "nowrap", background: C.surface2 },
  td:       { padding: "12px 14px", borderBottom: "1px solid rgba(255,255,255,.04)" },
  flexCard: { background: C.surface2, border: `1px solid ${C.border}`, borderRadius: 12, padding: "14px 18px", minWidth: 120 },
};

// ── Component ────────────────────────────────────────────────────────────────
export default function PayoutCalculator() {
  const [tab, setTab] = useState("builder");
  const [stake, setStake] = useState(10);
  const [legs, setLegs] = useState(["Standard", "Standard", "Standard"]);
  const [mode, setMode] = useState("estimate");
  const [exactPower, setExactPower] = useState("");
  const [exactFlex, setExactFlex]   = useState("");
  const [refLegs, setRefLegs] = useState(4);
  const [showSettings, setShowSettings] = useState(false);

  const [powerBase, setPowerBase] = useState(DEFAULT_POWER_BASE);
  const [flexBase, setFlexBase]   = useState(DEFAULT_FLEX_BASE);
  const [goblinPower, setGoblinPower] = useState(DEFAULT_GOBLIN_POWER);
  const [goblinFlex, setGoblinFlex]   = useState(DEFAULT_GOBLIN_FLEX);
  const [demonPower, setDemonPower]   = useState(DEFAULT_DEMON_POWER);
  const [demonFlex, setDemonFlex]     = useState(DEFAULT_DEMON_FLEX);

  useEffect(() => {
    try {
      const s = JSON.parse(localStorage.getItem(SETTINGS_KEY));
      if (!s) return;
      if (s.powerBase)   setPowerBase(s.powerBase);
      if (s.flexBase)    setFlexBase(s.flexBase);
      if (s.goblinPower) setGoblinPower(s.goblinPower);
      if (s.goblinFlex)  setGoblinFlex(s.goblinFlex);
      if (s.demonPower)  setDemonPower(s.demonPower);
      if (s.demonFlex)   setDemonFlex(s.demonFlex);
    } catch {}
  }, []);

  const mods = { goblinPower, goblinFlex, demonPower, demonFlex };
  const tables = { powerBase, flexBase };

  const result = useMemo(() => calcPayouts({
    legs,
    stake: Math.max(1, Number(stake) || 10),
    mode,
    exactPower: parseFloat(exactPower) || 0,
    exactFlex:  parseFloat(exactFlex) || 0,
    tables, mods,
  }), [legs, stake, mode, exactPower, exactFlex, powerBase, flexBase, goblinPower, goblinFlex, demonPower, demonFlex]);

  const badgeCounts = useMemo(() => {
    const c = { Standard: 0, Goblin: 0, Demon: 0 };
    legs.forEach(l => { const { kind } = classify(l); c[kind === "goblin" ? "Goblin" : kind === "demon" ? "Demon" : "Standard"]++; });
    return c;
  }, [legs]);

  const badgeStyle = {
    Goblin:   S.badge("rgba(168,85,247,.10)", "rgba(168,85,247,.25)", C.purple),
    Demon:    S.badge("rgba(239,68,68,.10)",  "rgba(239,68,68,.25)",  C.red),
    Standard: S.badge("rgba(59,130,246,.10)", "rgba(59,130,246,.25)", C.blue),
  };

  const tableRows = LEG_TYPES;

  return (
    <div style={S.page}>
      <style>{`select option { background: #18181c; } button:hover { opacity: 0.88; }`}</style>

      {/* HEADER */}
      <div style={S.header}>
        <div style={S.logo}>
          <div style={S.logoIcon}>🎯</div>
          <div>
            <div style={S.logoH1}>Payout Calculator</div>
            <div style={S.logoSub}>POWER · FLEX · STANDARD · GOBLIN · DEMON</div>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={{ fontFamily: mono, fontSize: 11, color: C.muted }}>STAKE $</span>
          <input
            type="number" min={1} value={stake}
            onChange={e => setStake(e.target.value)}
            style={S.input}
          />
        </div>
      </div>

      {/* TABS */}
      <div style={S.tabs}>
        {[["builder","🏗 BUILDER"],["reference","📊 REFERENCE"]].map(([id,label]) => (
          <button key={id} style={S.tabBtn(tab === id)} onClick={() => setTab(id)}>{label}</button>
        ))}
      </div>

      <div style={S.wrap}>
        {tab === "builder" && (
          <div style={S.grid2}>
            {/* LEFT — Leg Builder */}
            <div>
              <div style={S.secLabel}>TICKET · {legs.length} LEGS</div>

              {/* Settings Toggle */}
              <button
                onClick={() => setShowSettings(s => !s)}
                style={{ display: "inline-flex", alignItems: "center", gap: 6, background: C.surface2, border: `1px solid ${C.border2}`, color: C.muted, borderRadius: 8, padding: "7px 12px", cursor: "pointer", fontFamily: mono, fontSize: 11, marginBottom: 12, transition: "all .15s" }}
              >
                ⚙ Settings
              </button>

              {showSettings && (
                <div style={{ marginBottom: 16, border: `1px solid ${C.border}`, borderRadius: 12, background: C.surface2, padding: 14 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 10, marginBottom: 10 }}>
                    <span style={{ fontSize: 12, color: C.muted }}>Update payout tables / modifiers</span>
                    <div style={{ display: "flex", gap: 8 }}>
                      {["Reset defaults"].map(label => (
                        <button key={label} onClick={() => { setPowerBase(DEFAULT_POWER_BASE); setFlexBase(DEFAULT_FLEX_BASE); setGoblinPower(DEFAULT_GOBLIN_POWER); setGoblinFlex(DEFAULT_GOBLIN_FLEX); setDemonPower(DEFAULT_DEMON_POWER); setDemonFlex(DEFAULT_DEMON_FLEX); }}
                          style={{ background: C.surface3, border: `1px solid ${C.border2}`, color: C.text, borderRadius: 7, padding: "6px 12px", cursor: "pointer", fontFamily: mono, fontSize: 11 }}>
                          {label}
                        </button>
                      ))}
                    </div>
                  </div>
                  {/* Power base */}
                  <div style={{ fontFamily: mono, fontSize: 10, color: C.muted, letterSpacing: "1.5px", marginBottom: 8 }}>STANDARD POWER MULTIPLIERS</div>
                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginBottom: 12 }}>
                    {[2,3,4,5,6].map(n => (
                      <div key={n} style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <span style={{ fontFamily: mono, fontSize: 11, color: C.muted, width: 40 }}>{n}-leg</span>
                        <input value={powerBase[n]} onChange={e => { const x = parseFloat(e.target.value); if (!isNaN(x)) setPowerBase(p => ({...p,[n]:x})); }}
                          style={{ width: 72, background: C.bg, color: C.text, border: `1px solid ${C.border2}`, borderRadius: 7, padding: "5px 8px", fontFamily: mono, fontSize: 12, outline: "none" }}/>
                      </div>
                    ))}
                  </div>
                  {/* Estimate modifiers */}
                  <div style={{ fontFamily: mono, fontSize: 10, color: C.muted, letterSpacing: "1.5px", marginBottom: 8 }}>ESTIMATE MODIFIERS (PER LEG)</div>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
                    {[["Goblin Power","goblinPower",goblinPower,setGoblinPower],["Goblin Flex","goblinFlex",goblinFlex,setGoblinFlex],["Demon Power","demonPower",demonPower,setDemonPower],["Demon Flex","demonFlex",demonFlex,setDemonFlex]].map(([label,key,obj,setter]) => (
                      <div key={key} style={{ background: C.bg, border: `1px solid ${C.border}`, borderRadius: 8, padding: "10px 12px" }}>
                        <div style={{ fontFamily: mono, fontSize: 10, color: C.muted, marginBottom: 8 }}>{label}</div>
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          {[1,2,3].map(dev => (
                            <div key={dev} style={{ display: "flex", alignItems: "center", gap: 6 }}>
                              <span style={{ fontFamily: mono, fontSize: 11, color: C.muted, width: 16 }}>{dev}</span>
                              <input value={obj[dev]} onChange={e => { const x = parseFloat(e.target.value); if (!isNaN(x)) setter(p => ({...p,[dev]:x})); }}
                                style={{ width: 62, background: C.surface2, color: C.text, border: `1px solid ${C.border2}`, borderRadius: 6, padding: "4px 7px", fontFamily: mono, fontSize: 12, outline: "none" }}/>
                            </div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Mode toggle */}
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 12, flexWrap: "wrap" }}>
                <span style={{ fontFamily: mono, fontSize: 10, color: C.muted, letterSpacing: "1px" }}>MODE:</span>
                {["estimate","exact"].map(m => (
                  <button key={m} onClick={() => setMode(m)} style={S.smallBtn(mode === m)}>
                    {m === "estimate" ? "Estimate" : "Exact Override"}
                  </button>
                ))}
              </div>

              {mode === "exact" && (
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginBottom: 14 }}>
                  {[["Power Mult", exactPower, setExactPower], ["Flex Mult", exactFlex, setExactFlex]].map(([label, val, set]) => (
                    <div key={label}>
                      <div style={{ fontFamily: mono, fontSize: 10, color: C.muted, marginBottom: 6 }}>{label.toUpperCase()}</div>
                      <input placeholder="e.g. 45" value={val} onChange={e => set(e.target.value)}
                        style={{ width: "100%", background: C.surface2, color: C.text, border: `1px solid ${C.border2}`, borderRadius: 8, padding: "8px 10px", fontFamily: mono, fontSize: 13, outline: "none" }}/>
                    </div>
                  ))}
                </div>
              )}

              {/* Legs */}
              {legs.map((leg, i) => (
                <div key={i} style={S.legRow(legColor(leg))}>
                  <span style={{ fontSize: 15, width: 22, textAlign: "center" }}>{legEmoji(leg)}</span>
                  <span style={S.legNum}>LEG {i+1}</span>
                  <select value={leg} onChange={e => setLegs(p => p.map((x,j) => j===i ? e.target.value : x))} style={S.legSel(legColor(leg))}>
                    {LEG_TYPES.map(t => <option key={t} value={t}>{legEmoji(t)} {t}</option>)}
                  </select>
                  <button style={S.rmBtn} onClick={() => { if (legs.length > 2) setLegs(p => p.filter((_,j) => j !== i)); }}>×</button>
                </div>
              ))}

              {legs.length < 6 && (
                <button style={S.addBtn} onClick={() => setLegs(p => [...p, "Standard"])}>+ ADD LEG</button>
              )}

              <div style={{ display: "flex", gap: 6, marginTop: 14, flexWrap: "wrap" }}>
                {Object.entries(badgeCounts).filter(([,v]) => v > 0).map(([k,v]) => (
                  <span key={k} style={badgeStyle[k]}>{v}× {k}</span>
                ))}
              </div>
            </div>

            {/* RIGHT — Results */}
            <div>
              {!result ? (
                <div style={{ color: C.dim, textAlign: "center", padding: "60px 20px" }}>
                  <div style={{ fontSize: 32, marginBottom: 10, opacity: .5 }}>📊</div>
                  <div style={{ fontFamily: mono, fontSize: 12 }}>Add at least 2 legs to see payouts</div>
                </div>
              ) : (
                <>
                  <div style={S.secLabel}>RESULTS · ${money(Math.max(1, Number(stake)||10))} STAKE</div>

                  {/* Power Card */}
                  <div style={S.card("rgba(59,130,246,.08)","rgba(59,130,246,.02)","rgba(59,130,246,.20)")}>
                    <div style={S.cardHead}>
                      <span style={S.tag("rgba(59,130,246,.18)", C.blue, "rgba(59,130,246,.28)")}>POWER PLAY</span>
                      <span style={S.cardSub}>All {result.n} correct to win</span>
                    </div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10 }}>
                      <div style={S.metBox}><div style={S.metLabel}>MULTIPLIER</div><div style={S.metVal(C.blue)}>{result.powerMult}x</div></div>
                      <div style={S.metBox}><div style={S.metLabel}>WIN AMOUNT</div><div style={S.metVal(C.green)}>${money(result.powerWin)}</div></div>
                      <div style={S.metBox}>
                        <div style={S.metLabel}>TO WIN $100</div>
                        <div style={S.metVal(C.gold, 20)}>${money(result.breakeven)}</div>
                        <div style={S.metSub}>Breakeven {result.breakeven}%</div>
                      </div>
                    </div>
                    <div style={{ marginTop: 10, fontFamily: mono, fontSize: 10, color: C.muted }}>
                      Base: {result.basePower}x · Mod: {r2(result.pm)}x {mode === "exact" ? "(override)" : "(est)"}
                    </div>
                  </div>

                  {/* Flex Card */}
                  <div style={S.card("rgba(168,85,247,.08)","rgba(168,85,247,.02)","rgba(168,85,247,.20)")}>
                    <div style={S.cardHead}>
                      <span style={S.tag("rgba(168,85,247,.18)", C.purple, "rgba(168,85,247,.28)")}>FLEX PLAY</span>
                      <span style={S.cardSub}>Partial wins allowed</span>
                    </div>
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
                      <div style={S.metBox}><div style={S.metLabel}>{result.n}/{result.n} CORRECT</div><div style={S.metVal(C.purple)}>{result.flexMult}x</div></div>
                      <div style={S.metBox}><div style={S.metLabel}>WIN AMOUNT</div><div style={S.metVal(C.green)}>${money(result.flexWin)}</div></div>
                    </div>
                    {result.partials.length > 0 && (
                      <div style={S.partWrap}>
                        <div style={{ fontFamily: mono, fontSize: 9, color: C.muted, letterSpacing: "2px", marginBottom: 8 }}>PARTIAL PAYOUTS</div>
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          {result.partials.map(p => (
                            <div key={p.correct} style={S.partBox}>
                              <div style={{ color: C.purple, fontFamily: mono, fontSize: 11, marginBottom: 3 }}>{p.correct}/{result.n}</div>
                              <div style={{ fontFamily: syne, fontSize: 18, fontWeight: 700 }}>{p.adj}x</div>
                              <div style={{ color: C.green, fontFamily: mono, fontSize: 11, marginTop: 4 }}>${money(p.win)}</div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                    <div style={{ marginTop: 10, fontFamily: mono, fontSize: 10, color: C.muted }}>
                      Base top: {result.baseFlexTop}x · Mod: {r2(result.fm)}x {mode === "exact" ? "(override)" : "(est)"}
                    </div>
                  </div>

                  {result.pm !== 1 && (
                    <div style={S.modBar}>
                      <div style={{ fontFamily: mono, fontSize: 10, color: C.muted }}>
                        Composite mod — Power: {r2(result.pm)}x · Flex: {r2(result.fm)}x
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        )}

        {/* REFERENCE TAB */}
        {tab === "reference" && (
          <div>
            <div style={S.secLabel}>REFERENCE · STANDARD PAYOUTS</div>
            <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 20, flexWrap: "wrap" }}>
              <span style={{ fontFamily: mono, fontSize: 10, color: C.muted, letterSpacing: "2px" }}>LEG COUNT:</span>
              {[2,3,4,5,6].map(n => (
                <button key={n} style={S.smallBtn(refLegs === n)} onClick={() => setRefLegs(n)}>{n}-LEG</button>
              ))}
            </div>
            <div style={{ overflowX: "auto" }}>
              <table style={S.table}>
                <thead>
                  <tr>
                    {["TYPE","POWER","WIN $"+(Math.max(1,Number(stake)||10))+" (PP)","TO WIN $100","FLEX","WIN $"+(Math.max(1,Number(stake)||10))+" (FLEX)"].map((h,i) => (
                      <th key={h} style={{ ...S.th, textAlign: i === 0 ? "left" : "center" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {tableRows.map((row, idx) => {
                    const p = calcPayouts({ legs: Array(refLegs).fill(row), stake: Math.max(1,Number(stake)||10), mode: "estimate", exactPower: 0, exactFlex: 0, tables, mods });
                    if (!p) return null;
                    const color = legColor(row);
                    return (
                      <tr key={row} style={{ background: idx%2===0 ? "rgba(255,255,255,.015)" : "transparent" }}>
                        <td style={{ ...S.td, color, fontFamily: mono, fontSize: 12, fontWeight: 500 }}>{legEmoji(row)} {row}</td>
                        <td style={{ ...S.td, textAlign: "center", color: C.blue, fontFamily: mono, fontWeight: 500 }}>{p.powerMult}x</td>
                        <td style={{ ...S.td, textAlign: "center", color: C.green, fontFamily: mono }}>${money(p.powerWin)}</td>
                        <td style={{ ...S.td, textAlign: "center", color: C.gold, fontFamily: mono }}>${money(p.breakeven)}</td>
                        <td style={{ ...S.td, textAlign: "center", color: C.purple, fontFamily: mono, fontWeight: 500 }}>{p.flexMult}x</td>
                        <td style={{ ...S.td, textAlign: "center", color: C.green, fontFamily: mono }}>${money(p.flexWin)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            <div style={{ marginTop: 32 }}>
              <div style={S.secLabel}>FLEX PARTIAL PAYOUTS — STANDARD</div>
              <div style={{ display: "flex", gap: 12, flexWrap: "wrap" }}>
                {[2,3,4,5,6].map(n => (
                  <div key={n} style={S.flexCard}>
                    <div style={{ fontFamily: syne, color: C.blue, fontWeight: 700, marginBottom: 10, fontSize: 13 }}>{n}-Leg Flex</div>
                    {Object.entries(flexBase[n]||{}).sort((a,b)=>b[0]-a[0]).map(([k,v]) => (
                      <div key={k} style={{ display: "flex", justifyContent: "space-between", marginBottom: 6, gap: 16 }}>
                        <span style={{ fontFamily: mono, color: C.muted, fontSize: 11 }}>{k}/{n}</span>
                        <span style={{ fontFamily: mono, color: C.text, fontWeight: 500, fontSize: 11 }}>{v}x</span>
                      </div>
                    ))}
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
