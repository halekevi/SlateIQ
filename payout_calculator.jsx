import { useState, useCallback } from "react";

// ── Payout Data ────────────────────────────────────────────────────────────────
const POWER_BASE = { 2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 37.5 };
const FLEX_BASE = {
  2: { 2: 3.0 },
  3: { 3: 3.0, 2: 1.0 },
  4: { 4: 6.0, 3: 1.5 },
  5: { 5: 10.0, 4: 2.0, 3: 0.4 },
  6: { 6: 25.0, 5: 2.0, 4: 0.4 },
};

// Goblin modifiers by deviation (1=closest to standard, 3=furthest)
const GOBLIN_POWER = { 1: 0.840, 2: 0.747, 3: 0.707 };
const GOBLIN_FLEX  = { 1: 0.800, 2: 0.720, 3: 0.600 };
// Demon modifiers
const DEMON_POWER  = { 1: 1.627, 2: 2.400, 3: 2.720 };
const DEMON_FLEX   = { 1: 1.600, 2: 1.520, 3: 1.560 };

const LEG_TYPES = ["Standard", "Goblin -1", "Goblin -2", "Goblin -3", "Demon +1", "Demon +2", "Demon +3"];

function classifyLeg(legType) {
  if (legType.startsWith("Goblin")) {
    const dev = parseInt(legType.split("-")[1]);
    return { kind: "goblin", dev };
  }
  if (legType.startsWith("Demon")) {
    const dev = parseInt(legType.split("+")[1]);
    return { kind: "demon", dev };
  }
  return { kind: "standard", dev: 0 };
}

function calcPayouts(legs) {
  const n = legs.length;
  if (n < 2) return null;

  let powerMod = 1.0;
  let flexMod  = 1.0;

  for (const leg of legs) {
    const { kind, dev } = classifyLeg(leg);
    if (kind === "goblin") {
      powerMod *= GOBLIN_POWER[dev] ?? 0.840;
      flexMod  *= GOBLIN_FLEX[dev]  ?? 0.800;
    } else if (kind === "demon") {
      powerMod *= DEMON_POWER[dev] ?? 1.627;
      flexMod  *= DEMON_FLEX[dev]  ?? 1.600;
    }
  }

  const powerTop = parseFloat((POWER_BASE[n] * powerMod).toFixed(2));
  const flexFull = FLEX_BASE[n] ?? {};
  const flexTop  = parseFloat(((flexFull[n] ?? 25) * flexMod).toFixed(2));

  const flexPartials = Object.entries(flexFull)
    .filter(([k]) => parseInt(k) < n)
    .map(([k, v]) => ({ correct: parseInt(k), mult: v }));

  return { n, powerTop, flexTop, flexPartials, powerMod, flexMod };
}

// ── Color helpers ──────────────────────────────────────────────────────────────
function legColor(legType) {
  if (legType.startsWith("Goblin")) return "#7c3aed";
  if (legType.startsWith("Demon"))  return "#dc2626";
  return "#1d4ed8";
}

function legEmoji(legType) {
  if (legType.startsWith("Goblin")) return "👺";
  if (legType.startsWith("Demon"))  return "😈";
  return "⭐";
}

// ── Components ─────────────────────────────────────────────────────────────────
function LegSlot({ index, value, onChange, onRemove }) {
  return (
    <div style={{
      display: "flex", alignItems: "center", gap: 10, marginBottom: 8,
      background: "rgba(255,255,255,0.04)", borderRadius: 10, padding: "8px 12px",
      border: `1.5px solid ${legColor(value)}44`,
    }}>
      <span style={{ fontSize: 18, width: 28, textAlign: "center" }}>{legEmoji(value)}</span>
      <span style={{ color: "#94a3b8", fontSize: 12, width: 50, flexShrink: 0 }}>Leg {index + 1}</span>
      <select
        value={value}
        onChange={e => onChange(index, e.target.value)}
        style={{
          flex: 1, background: "#0f172a", color: "#f8fafc", border: `1px solid ${legColor(value)}66`,
          borderRadius: 8, padding: "6px 10px", fontSize: 13, fontFamily: "'Space Mono', monospace",
          cursor: "pointer", outline: "none",
        }}
      >
        {LEG_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
      </select>
      <button
        onClick={() => onRemove(index)}
        style={{
          background: "rgba(239,68,68,0.15)", color: "#ef4444", border: "none",
          borderRadius: 6, padding: "4px 10px", cursor: "pointer", fontSize: 16, fontWeight: "bold",
        }}
      >×</button>
    </div>
  );
}

function PayoutCard({ label, value, sub, accent, big }) {
  return (
    <div style={{
      background: `linear-gradient(135deg, ${accent}18, ${accent}08)`,
      border: `1px solid ${accent}33`, borderRadius: 12, padding: big ? "18px 22px" : "14px 18px",
      textAlign: "center", flex: 1,
    }}>
      <div style={{ color: "#64748b", fontSize: 11, letterSpacing: 2, textTransform: "uppercase", marginBottom: 4 }}>{label}</div>
      <div style={{ color: accent, fontSize: big ? 32 : 24, fontFamily: "'Space Mono', monospace", fontWeight: "bold" }}>{value}</div>
      {sub && <div style={{ color: "#64748b", fontSize: 11, marginTop: 4 }}>{sub}</div>}
    </div>
  );
}

function QuickTable() {
  const [selectedLegs, setSelectedLegs] = useState(3);
  const rows = ["Standard", "Goblin -1", "Goblin -2", "Goblin -3", "Demon +1", "Demon +2", "Demon +3"];

  return (
    <div style={{ marginTop: 32 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 16, marginBottom: 16 }}>
        <span style={{ color: "#94a3b8", fontSize: 13, letterSpacing: 1 }}>QUICK REFERENCE — LEGS:</span>
        {[3, 4, 5, 6].map(n => (
          <button key={n} onClick={() => setSelectedLegs(n)} style={{
            background: selectedLegs === n ? "#3b82f6" : "rgba(255,255,255,0.06)",
            color: selectedLegs === n ? "#fff" : "#94a3b8",
            border: "none", borderRadius: 8, padding: "5px 14px", cursor: "pointer",
            fontSize: 13, fontFamily: "'Space Mono', monospace", fontWeight: "bold",
            transition: "all 0.15s",
          }}>{n}-Leg</button>
        ))}
      </div>
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
          <thead>
            <tr>
              {["Leg Type", "Power Payout", "$10 stake wins", "Flex Top", "$10 stake wins"].map(h => (
                <th key={h} style={{
                  padding: "10px 14px", textAlign: "center", color: "#475569",
                  borderBottom: "1px solid #1e293b", fontSize: 11, letterSpacing: 1.5,
                  textTransform: "uppercase", whiteSpace: "nowrap",
                }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, i) => {
              const legs = Array(selectedLegs).fill(row);
              const p = calcPayouts(legs);
              if (!p) return null;
              const even = i % 2 === 0;
              return (
                <tr key={row} style={{ background: even ? "rgba(255,255,255,0.02)" : "transparent" }}>
                  <td style={{ padding: "10px 14px", color: legColor(row), fontFamily: "'Space Mono', monospace", fontWeight: "bold" }}>
                    {legEmoji(row)} {row}
                  </td>
                  <td style={{ padding: "10px 14px", textAlign: "center", color: "#f8fafc", fontFamily: "'Space Mono', monospace" }}>
                    {p.powerTop}x
                  </td>
                  <td style={{ padding: "10px 14px", textAlign: "center", color: "#10b981", fontFamily: "'Space Mono', monospace" }}>
                    ${(10 * p.powerTop).toFixed(2)}
                  </td>
                  <td style={{ padding: "10px 14px", textAlign: "center", color: "#f8fafc", fontFamily: "'Space Mono', monospace" }}>
                    {p.flexTop}x
                  </td>
                  <td style={{ padding: "10px 14px", textAlign: "center", color: "#10b981", fontFamily: "'Space Mono', monospace" }}>
                    ${(10 * p.flexTop).toFixed(2)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Main App ───────────────────────────────────────────────────────────────────
export default function App() {
  const [legs, setLegs] = useState(["Standard", "Standard", "Standard"]);
  const [stake, setStake] = useState(10);
  const [activeTab, setActiveTab] = useState("builder");

  const addLeg = useCallback(() => {
    if (legs.length < 6) setLegs(l => [...l, "Standard"]);
  }, [legs]);

  const removeLeg = useCallback((i) => {
    if (legs.length > 2) setLegs(l => l.filter((_, idx) => idx !== i));
  }, [legs]);

  const changeLeg = useCallback((i, val) => {
    setLegs(l => l.map((v, idx) => idx === i ? val : v));
  }, []);

  const result = calcPayouts(legs);

  const composition = legs.reduce((acc, l) => {
    const k = l.startsWith("Goblin") ? "Goblin" : l.startsWith("Demon") ? "Demon" : "Standard";
    acc[k] = (acc[k] || 0) + 1;
    return acc;
  }, {});

  return (
    <div style={{
      minHeight: "100vh", background: "#020817",
      fontFamily: "'Space Grotesk', sans-serif",
      padding: "0 0 60px 0",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Space+Grotesk:wght@300;400;500;600;700&display=swap');
        * { box-sizing: border-box; }
        select option { background: #0f172a; }
        input[type=number]::-webkit-inner-spin-button { opacity: 1; }
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: #0f172a; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
      `}</style>

      {/* Header */}
      <div style={{
        background: "linear-gradient(180deg, #0f172a 0%, #020817 100%)",
        borderBottom: "1px solid #1e293b", padding: "28px 32px 24px",
        display: "flex", alignItems: "center", justifyContent: "space-between",
      }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div style={{
              background: "linear-gradient(135deg, #3b82f6, #7c3aed)",
              borderRadius: 10, padding: "6px 12px", fontSize: 20,
            }}>🎯</div>
            <div>
              <div style={{ color: "#f8fafc", fontSize: 22, fontWeight: 700, letterSpacing: -0.5 }}>
                PrizePicks Payout Calculator
              </div>
              <div style={{ color: "#475569", fontSize: 12, marginTop: 2 }}>
                Power Play & Flex — Standard, Goblin, Demon
              </div>
            </div>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <span style={{ color: "#475569", fontSize: 12 }}>Stake $</span>
          <input
            type="number" value={stake} min={1} max={10000}
            onChange={e => setStake(Math.max(1, parseFloat(e.target.value) || 1))}
            style={{
              width: 80, background: "#0f172a", color: "#f8fafc",
              border: "1px solid #334155", borderRadius: 8, padding: "6px 10px",
              fontSize: 14, fontFamily: "'Space Mono', monospace", outline: "none",
            }}
          />
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 0, borderBottom: "1px solid #1e293b", background: "#0a1120", padding: "0 32px" }}>
        {[["builder", "🏗 Ticket Builder"], ["table", "📊 Quick Reference"]].map(([id, label]) => (
          <button key={id} onClick={() => setActiveTab(id)} style={{
            background: "none", border: "none", borderBottom: activeTab === id ? "2px solid #3b82f6" : "2px solid transparent",
            color: activeTab === id ? "#f8fafc" : "#475569", padding: "14px 20px",
            cursor: "pointer", fontSize: 13, fontWeight: 600, letterSpacing: 0.5,
            transition: "all 0.15s",
          }}>{label}</button>
        ))}
      </div>

      <div style={{ maxWidth: 860, margin: "0 auto", padding: "28px 24px" }}>

        {activeTab === "builder" && (
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>

            {/* Left: Leg Builder */}
            <div>
              <div style={{ color: "#64748b", fontSize: 11, letterSpacing: 2, textTransform: "uppercase", marginBottom: 14 }}>
                Ticket Composition · {legs.length} Legs
              </div>

              {legs.map((leg, i) => (
                <LegSlot key={i} index={i} value={leg} onChange={changeLeg} onRemove={removeLeg} />
              ))}

              {legs.length < 6 && (
                <button onClick={addLeg} style={{
                  width: "100%", padding: "10px", background: "rgba(59,130,246,0.08)",
                  border: "1.5px dashed #3b82f666", borderRadius: 10, color: "#3b82f6",
                  cursor: "pointer", fontSize: 13, fontWeight: 600, marginTop: 4,
                  transition: "all 0.15s",
                }}>+ Add Leg</button>
              )}

              {/* Composition badges */}
              <div style={{ display: "flex", gap: 8, marginTop: 16, flexWrap: "wrap" }}>
                {Object.entries(composition).map(([k, v]) => (
                  <div key={k} style={{
                    background: k === "Goblin" ? "#7c3aed22" : k === "Demon" ? "#dc262622" : "#1d4ed822",
                    border: `1px solid ${k === "Goblin" ? "#7c3aed44" : k === "Demon" ? "#dc262644" : "#1d4ed844"}`,
                    color: k === "Goblin" ? "#a78bfa" : k === "Demon" ? "#f87171" : "#60a5fa",
                    borderRadius: 20, padding: "3px 12px", fontSize: 12, fontWeight: 600,
                  }}>{v}× {k}</div>
                ))}
              </div>
            </div>

            {/* Right: Results */}
            <div>
              {result ? (
                <>
                  <div style={{ color: "#64748b", fontSize: 11, letterSpacing: 2, textTransform: "uppercase", marginBottom: 14 }}>
                    Payout Results · ${stake} Stake
                  </div>

                  {/* Power Play */}
                  <div style={{
                    background: "linear-gradient(135deg, #1e3a5f, #0f1e3a)",
                    border: "1px solid #1d4ed833", borderRadius: 14, padding: "18px 20px", marginBottom: 16,
                  }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
                      <div style={{ background: "#3b82f6", borderRadius: 6, padding: "2px 10px", fontSize: 11, color: "#fff", fontWeight: 700, letterSpacing: 1 }}>POWER PLAY</div>
                      <span style={{ color: "#475569", fontSize: 11 }}>All {result.n} must be correct</span>
                    </div>
                    <div style={{ display: "flex", gap: 12 }}>
                      <PayoutCard label="Multiplier" value={`${result.powerTop}x`} accent="#3b82f6" big />
                      <PayoutCard label={`Win on $${stake}`} value={`$${(stake * result.powerTop).toFixed(2)}`} accent="#10b981" big />
                      <PayoutCard label="Stake to win $100" value={`$${(100 / result.powerTop).toFixed(2)}`} accent="#f59e0b" />
                    </div>
                  </div>

                  {/* Flex Play */}
                  <div style={{
                    background: "linear-gradient(135deg, #2d1b4e, #1a0f2e)",
                    border: "1px solid #7c3aed33", borderRadius: 14, padding: "18px 20px",
                  }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
                      <div style={{ background: "#7c3aed", borderRadius: 6, padding: "2px 10px", fontSize: 11, color: "#fff", fontWeight: 700, letterSpacing: 1 }}>FLEX PLAY</div>
                      <span style={{ color: "#475569", fontSize: 11 }}>Partial wins allowed</span>
                    </div>
                    <div style={{ display: "flex", gap: 12, marginBottom: 14 }}>
                      <PayoutCard label={`${result.n}/${result.n} correct`} value={`${result.flexTop}x`} accent="#7c3aed" big />
                      <PayoutCard label={`Win on $${stake}`} value={`$${(stake * result.flexTop).toFixed(2)}`} accent="#10b981" big />
                    </div>
                    {result.flexPartials.length > 0 && (
                      <div style={{ borderTop: "1px solid #7c3aed22", paddingTop: 12 }}>
                        <div style={{ color: "#475569", fontSize: 11, marginBottom: 8, letterSpacing: 1 }}>PARTIAL PAYOUTS</div>
                        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                          {result.flexPartials.sort((a, b) => b.correct - a.correct).map(({ correct, mult }) => (
                            <div key={correct} style={{
                              background: "rgba(124,58,237,0.12)", border: "1px solid #7c3aed33",
                              borderRadius: 8, padding: "8px 14px", textAlign: "center",
                            }}>
                              <div style={{ color: "#a78bfa", fontSize: 11, marginBottom: 2 }}>{correct}/{result.n} correct</div>
                              <div style={{ color: "#f8fafc", fontFamily: "'Space Mono', monospace", fontSize: 15, fontWeight: 700 }}>{mult}x</div>
                              <div style={{ color: "#10b981", fontSize: 11, marginTop: 2 }}>${(stake * mult).toFixed(2)}</div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>

                  {/* Modifier breakdown */}
                  {(result.powerMod !== 1.0) && (
                    <div style={{ marginTop: 12, background: "rgba(255,255,255,0.03)", borderRadius: 10, padding: "10px 14px" }}>
                      <div style={{ color: "#475569", fontSize: 11, letterSpacing: 1 }}>
                        DEVIATION MODIFIERS · Power: {(result.powerMod).toFixed(3)}x base · Flex: {(result.flexMod).toFixed(3)}x base
                      </div>
                    </div>
                  )}
                </>
              ) : (
                <div style={{ color: "#475569", textAlign: "center", marginTop: 60 }}>
                  Add at least 2 legs to see payouts
                </div>
              )}
            </div>
          </div>
        )}

        {activeTab === "table" && <QuickTable />}
      </div>
    </div>
  );
}
