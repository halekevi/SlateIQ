import { useState, useCallback } from "react";

const POWER_BASE = { 2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 37.5 };
const FLEX_BASE = {
  2: { 2: 3.0 },
  3: { 3: 3.0, 2: 1.0 },
  4: { 4: 6.0, 3: 1.5 },
  5: { 5: 10.0, 4: 2.0, 3: 0.4 },
  6: { 6: 25.0, 5: 2.0, 4: 0.4 },
};
const GOBLIN_POWER = { 1: 0.840, 2: 0.747, 3: 0.707 };
const GOBLIN_FLEX  = { 1: 0.800, 2: 0.720, 3: 0.600 };
const DEMON_POWER  = { 1: 1.627, 2: 2.400, 3: 2.720 };
const DEMON_FLEX   = { 1: 1.600, 2: 1.520, 3: 1.560 };
const LEG_TYPES = ["Standard","Goblin -1","Goblin -2","Goblin -3","Demon +1","Demon +2","Demon +3"];

function classifyLeg(t) {
  if (t.startsWith("Goblin")) return { kind:"goblin", dev: parseInt(t.split("-")[1]) };
  if (t.startsWith("Demon"))  return { kind:"demon",  dev: parseInt(t.split("+")[1]) };
  return { kind:"standard", dev:0 };
}

function calcPayouts(legs) {
  const n = legs.length;
  if (n < 2) return null;
  let pm = 1, fm = 1;
  for (const leg of legs) {
    const {kind,dev} = classifyLeg(leg);
    if (kind==="goblin") { pm *= GOBLIN_POWER[dev]??0.840; fm *= GOBLIN_FLEX[dev]??0.800; }
    else if (kind==="demon") { pm *= DEMON_POWER[dev]??1.627; fm *= DEMON_FLEX[dev]??1.600; }
  }
  const powerTop = parseFloat((POWER_BASE[n]*pm).toFixed(2));
  const flexFull = FLEX_BASE[n]??{};
  const flexTop  = parseFloat(((flexFull[n]??25)*fm).toFixed(2));
  const flexPartials = Object.entries(flexFull).filter(([k])=>parseInt(k)<n).map(([k,v])=>({correct:parseInt(k),mult:v}));
  return { n, powerTop, flexTop, flexPartials, pm, fm };
}

function legColor(t) {
  if (t.startsWith("Goblin")) return "#7c3aed";
  if (t.startsWith("Demon"))  return "#dc2626";
  return "#2563eb";
}
function legEmoji(t) {
  if (t.startsWith("Goblin")) return "👺";
  if (t.startsWith("Demon"))  return "😈";
  return "⭐";
}

export default function App() {
  const [legs, setLegs] = useState(["Standard","Standard","Standard"]);
  const [stake, setStake] = useState(10);
  const [tab, setTab] = useState("builder");
  const [tableLegs, setTableLegs] = useState(3);

  const addLeg = () => { if (legs.length < 6) setLegs(l=>[...l,"Standard"]); };
  const removeLeg = (i) => { if (legs.length > 2) setLegs(l=>l.filter((_,idx)=>idx!==i)); };
  const changeLeg = (i,v) => setLegs(l=>l.map((x,idx)=>idx===i?v:x));

  const result = calcPayouts(legs);
  const comp = legs.reduce((a,l)=>{ const k=l.startsWith("Goblin")?"Goblin":l.startsWith("Demon")?"Demon":"Standard"; a[k]=(a[k]||0)+1; return a; },{});

  const tableRows = ["Standard","Goblin -1","Goblin -2","Goblin -3","Demon +1","Demon +2","Demon +3"];

  return (
    <div style={{minHeight:"100vh",background:"#030712",fontFamily:"'Courier New',monospace",color:"#f1f5f9"}}>
      <style>{`
        select option{background:#0f172a}
        button:hover{opacity:0.85}
        .leg-row{transition:border-color 0.2s}
      `}</style>

      {/* Header */}
      <div style={{background:"#0a0f1e",borderBottom:"1px solid #1e293b",padding:"20px 28px",display:"flex",alignItems:"center",justifyContent:"space-between",flexWrap:"wrap",gap:12}}>
        <div>
          <div style={{fontSize:20,fontWeight:"bold",letterSpacing:1,color:"#f8fafc"}}>
            🎯 PRIZEPICKS PAYOUT CALC
          </div>
          <div style={{fontSize:11,color:"#475569",marginTop:2,letterSpacing:2}}>POWER PLAY · FLEX · STANDARD · GOBLIN · DEMON</div>
        </div>
        <div style={{display:"flex",alignItems:"center",gap:8}}>
          <span style={{color:"#64748b",fontSize:12}}>STAKE $</span>
          <input type="number" value={stake} min={1} max={100000}
            onChange={e=>setStake(Math.max(1,parseFloat(e.target.value)||1))}
            style={{width:80,background:"#0f172a",color:"#f8fafc",border:"1px solid #334155",borderRadius:6,padding:"6px 10px",fontSize:14,outline:"none"}}
          />
        </div>
      </div>

      {/* Tabs */}
      <div style={{display:"flex",borderBottom:"1px solid #1e293b",background:"#070d1a",padding:"0 28px"}}>
        {[["builder","🏗  BUILDER"],["table","📊  REFERENCE"]].map(([id,label])=>(
          <button key={id} onClick={()=>setTab(id)} style={{
            background:"none",border:"none",borderBottom:tab===id?"2px solid #3b82f6":"2px solid transparent",
            color:tab===id?"#f8fafc":"#475569",padding:"12px 18px",cursor:"pointer",
            fontSize:12,fontWeight:"bold",letterSpacing:1.5,
          }}>{label}</button>
        ))}
      </div>

      <div style={{maxWidth:900,margin:"0 auto",padding:"24px 20px"}}>

        {tab==="builder" && (
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:24}}>

            {/* Legs */}
            <div>
              <div style={{fontSize:10,color:"#475569",letterSpacing:3,marginBottom:14}}>TICKET · {legs.length} LEGS</div>
              {legs.map((leg,i)=>(
                <div key={i} className="leg-row" style={{display:"flex",alignItems:"center",gap:8,marginBottom:8,
                  background:"rgba(255,255,255,0.03)",borderRadius:8,padding:"8px 10px",
                  border:`1px solid ${legColor(leg)}33`}}>
                  <span style={{fontSize:16,width:24,textAlign:"center"}}>{legEmoji(leg)}</span>
                  <span style={{color:"#475569",fontSize:11,width:48,flexShrink:0}}>LEG {i+1}</span>
                  <select value={leg} onChange={e=>changeLeg(i,e.target.value)} style={{
                    flex:1,background:"#0f172a",color:legColor(leg),border:`1px solid ${legColor(leg)}55`,
                    borderRadius:6,padding:"5px 8px",fontSize:12,cursor:"pointer",outline:"none",fontWeight:"bold",
                  }}>
                    {LEG_TYPES.map(t=><option key={t} value={t}>{legEmoji(t)} {t}</option>)}
                  </select>
                  <button onClick={()=>removeLeg(i)} style={{
                    background:"rgba(239,68,68,0.12)",color:"#ef4444",border:"none",
                    borderRadius:5,padding:"3px 9px",cursor:"pointer",fontSize:15,fontWeight:"bold",
                  }}>×</button>
                </div>
              ))}
              {legs.length < 6 && (
                <button onClick={addLeg} style={{
                  width:"100%",padding:"9px",background:"rgba(59,130,246,0.07)",
                  border:"1px dashed #3b82f655",borderRadius:8,color:"#3b82f6",
                  cursor:"pointer",fontSize:12,fontWeight:"bold",letterSpacing:1,marginTop:4,
                }}>+ ADD LEG</button>
              )}
              <div style={{display:"flex",gap:6,marginTop:14,flexWrap:"wrap"}}>
                {Object.entries(comp).map(([k,v])=>(
                  <div key={k} style={{
                    background:k==="Goblin"?"#7c3aed18":k==="Demon"?"#dc262618":"#2563eb18",
                    border:`1px solid ${k==="Goblin"?"#7c3aed44":k==="Demon"?"#dc262644":"#2563eb44"}`,
                    color:k==="Goblin"?"#a78bfa":k==="Demon"?"#f87171":"#60a5fa",
                    borderRadius:20,padding:"3px 12px",fontSize:11,fontWeight:"bold",
                  }}>{v}× {k}</div>
                ))}
              </div>
            </div>

            {/* Results */}
            <div>
              {result ? (<>
                <div style={{fontSize:10,color:"#475569",letterSpacing:3,marginBottom:14}}>RESULTS · ${stake} STAKE</div>

                {/* Power Play */}
                <div style={{background:"linear-gradient(135deg,#0c1a3a,#071020)",border:"1px solid #2563eb33",borderRadius:12,padding:"16px 18px",marginBottom:14}}>
                  <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:12}}>
                    <div style={{background:"#2563eb",borderRadius:4,padding:"2px 9px",fontSize:10,color:"#fff",fontWeight:"bold",letterSpacing:1.5}}>POWER PLAY</div>
                    <span style={{color:"#475569",fontSize:11}}>All {result.n} correct</span>
                  </div>
                  <div style={{display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:10}}>
                    {[
                      ["MULTIPLIER", `${result.powerTop}x`, "#3b82f6"],
                      [`WIN ON $${stake}`, `$${(stake*result.powerTop).toFixed(2)}`, "#10b981"],
                      ["TO WIN $100", `$${(100/result.powerTop).toFixed(2)}`, "#f59e0b"],
                    ].map(([label,val,color])=>(
                      <div key={label} style={{background:"rgba(255,255,255,0.03)",borderRadius:8,padding:"10px",textAlign:"center"}}>
                        <div style={{color:"#475569",fontSize:9,letterSpacing:2,marginBottom:4}}>{label}</div>
                        <div style={{color,fontSize:20,fontWeight:"bold"}}>{val}</div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Flex Play */}
                <div style={{background:"linear-gradient(135deg,#1a0a3a,#0f0620)",border:"1px solid #7c3aed33",borderRadius:12,padding:"16px 18px"}}>
                  <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:12}}>
                    <div style={{background:"#7c3aed",borderRadius:4,padding:"2px 9px",fontSize:10,color:"#fff",fontWeight:"bold",letterSpacing:1.5}}>FLEX PLAY</div>
                    <span style={{color:"#475569",fontSize:11}}>Partial wins allowed</span>
                  </div>
                  <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:10,marginBottom:result.flexPartials.length?12:0}}>
                    {[
                      [`${result.n}/${result.n} CORRECT`, `${result.flexTop}x`, "#7c3aed"],
                      [`WIN ON $${stake}`, `$${(stake*result.flexTop).toFixed(2)}`, "#10b981"],
                    ].map(([label,val,color])=>(
                      <div key={label} style={{background:"rgba(255,255,255,0.03)",borderRadius:8,padding:"10px",textAlign:"center"}}>
                        <div style={{color:"#475569",fontSize:9,letterSpacing:2,marginBottom:4}}>{label}</div>
                        <div style={{color,fontSize:20,fontWeight:"bold"}}>{val}</div>
                      </div>
                    ))}
                  </div>
                  {result.flexPartials.length>0 && (
                    <div style={{borderTop:"1px solid #7c3aed22",paddingTop:10}}>
                      <div style={{color:"#475569",fontSize:9,letterSpacing:2,marginBottom:8}}>PARTIAL PAYOUTS</div>
                      <div style={{display:"flex",gap:8,flexWrap:"wrap"}}>
                        {result.flexPartials.sort((a,b)=>b.correct-a.correct).map(({correct,mult})=>(
                          <div key={correct} style={{background:"rgba(124,58,237,0.1)",border:"1px solid #7c3aed33",borderRadius:8,padding:"8px 12px",textAlign:"center"}}>
                            <div style={{color:"#a78bfa",fontSize:10,marginBottom:2}}>{correct}/{result.n}</div>
                            <div style={{color:"#f8fafc",fontSize:16,fontWeight:"bold"}}>{mult}x</div>
                            <div style={{color:"#10b981",fontSize:10,marginTop:2}}>${(stake*mult).toFixed(2)}</div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>

                {result.pm !== 1.0 && (
                  <div style={{marginTop:10,background:"rgba(255,255,255,0.02)",borderRadius:8,padding:"8px 12px"}}>
                    <div style={{color:"#475569",fontSize:10,letterSpacing:1}}>
                      MOD: Power {result.pm.toFixed(3)}x base · Flex {result.fm.toFixed(3)}x base
                    </div>
                  </div>
                )}
              </>) : (
                <div style={{color:"#475569",textAlign:"center",marginTop:80,fontSize:13}}>Add at least 2 legs to see payouts</div>
              )}
            </div>
          </div>
        )}

        {tab==="table" && (
          <div>
            <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:18,flexWrap:"wrap"}}>
              <span style={{color:"#475569",fontSize:11,letterSpacing:2}}>LEG COUNT:</span>
              {[3,4,5,6].map(n=>(
                <button key={n} onClick={()=>setTableLegs(n)} style={{
                  background:tableLegs===n?"#3b82f6":"rgba(255,255,255,0.05)",
                  color:tableLegs===n?"#fff":"#64748b",border:"none",borderRadius:6,
                  padding:"5px 14px",cursor:"pointer",fontSize:12,fontWeight:"bold",letterSpacing:1,
                }}>{n}-LEG</button>
              ))}
              <span style={{color:"#334155",fontSize:11,marginLeft:8}}>Stake: ${stake}</span>
            </div>
            <div style={{overflowX:"auto"}}>
              <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
                <thead>
                  <tr>
                    {["LEG TYPE","POWER","WIN $"+stake+" (PP)","$ TO WIN $100","FLEX","WIN $"+stake+" (FL)"].map(h=>(
                      <th key={h} style={{padding:"10px 14px",textAlign:"center",color:"#334155",
                        borderBottom:"1px solid #1e293b",fontSize:10,letterSpacing:2,whiteSpace:"nowrap"}}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {tableRows.map((row,i)=>{
                    const legs = Array(tableLegs).fill(row);
                    const p = calcPayouts(legs);
                    if (!p) return null;
                    return (
                      <tr key={row} style={{background:i%2===0?"rgba(255,255,255,0.02)":"transparent",borderBottom:"1px solid #0f172a"}}>
                        <td style={{padding:"11px 14px",color:legColor(row),fontWeight:"bold"}}>
                          {legEmoji(row)} {row}
                        </td>
                        <td style={{padding:"11px 14px",textAlign:"center",color:"#60a5fa",fontWeight:"bold"}}>{p.powerTop}x</td>
                        <td style={{padding:"11px 14px",textAlign:"center",color:"#10b981",fontWeight:"bold"}}>${(stake*p.powerTop).toFixed(2)}</td>
                        <td style={{padding:"11px 14px",textAlign:"center",color:"#f59e0b"}}>${(100/p.powerTop).toFixed(2)}</td>
                        <td style={{padding:"11px 14px",textAlign:"center",color:"#a78bfa",fontWeight:"bold"}}>{p.flexTop}x</td>
                        <td style={{padding:"11px 14px",textAlign:"center",color:"#10b981"}}>${(stake*p.flexTop).toFixed(2)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            {/* Partial payouts reference */}
            <div style={{marginTop:28}}>
              <div style={{fontSize:10,color:"#475569",letterSpacing:3,marginBottom:14}}>FLEX PARTIAL PAYOUTS — STANDARD LINES</div>
              <div style={{display:"flex",gap:14,flexWrap:"wrap"}}>
                {[3,4,5,6].map(n=>(
                  <div key={n} style={{background:"rgba(255,255,255,0.03)",border:"1px solid #1e293b",borderRadius:10,padding:"14px 18px",minWidth:120}}>
                    <div style={{color:"#3b82f6",fontWeight:"bold",marginBottom:10,fontSize:13}}>{n}-LEG FLEX</div>
                    {Object.entries(FLEX_BASE[n]).sort((a,b)=>b[0]-a[0]).map(([k,v])=>(
                      <div key={k} style={{display:"flex",justifyContent:"space-between",marginBottom:6,gap:16}}>
                        <span style={{color:"#64748b",fontSize:11}}>{k}/{n}</span>
                        <span style={{color:"#f8fafc",fontWeight:"bold",fontSize:11}}>{v}x</span>
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
