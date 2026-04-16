#!/usr/bin/env node
"use strict";

const pptxgen = require("pptxgenjs");
const fs = require("fs");

// Leer datos del archivo temporal
const data = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const { config, lotes, logo_b64, output_path } = data;

// Colores corporativos
const VERDE_OSC = "1B5E20";
const VERDE_MED = "2E7D32";
const VERDE_CLA = "C8E6C9";
const NARANJA   = "E65100";
const GRIS_TX   = "37474F";
const GRIS_CLR  = "F5F5F5";
const BLANCO    = "FFFFFF";

function fmt(n, dec) {
  try {
    n = parseFloat(n);
    if (isNaN(n)) return "-";
    let partes = n.toFixed(dec !== undefined ? dec : 2).split(".");
    let entero = partes[0].replace(/\B(?=(\d{3})+(?!\d))/g, ".");
    let decimal = partes[1] || "00";
    return entero + "," + decimal;
  } catch(e) { return String(n); }
}

const pres = new pptxgen();
pres.layout = "LAYOUT_16x9";  // 10" x 5.625"
pres.title = config.titulo || "Estimacion de Rendimiento";

const LOGO_DATA = "image/png;base64," + logo_b64;

// ── Función helper: encabezado de cada slide ──────────────
function addHeader(slide, titulo, subtitulo) {
  // Barra superior verde
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0, w: 10, h: 0.75,
    fill: { color: VERDE_OSC }, line: { color: VERDE_OSC }
  });
  // Línea naranja
  slide.addShape(pres.shapes.RECTANGLE, {
    x: 0, y: 0.75, w: 10, h: 0.05,
    fill: { color: NARANJA }, line: { color: NARANJA }
  });
  // Titulo
  slide.addText(titulo, {
    x: 0.3, y: 0, w: 8.2, h: 0.75,
    fontSize: 18, bold: true, color: BLANCO,
    valign: "middle", margin: 0
  });
  // Logo
  slide.addImage({ data: LOGO_DATA, x: 8.6, y: 0.05, w: 1.2, h: 0.65 });
  // Subtitulo
  if (subtitulo) {
    slide.addText(subtitulo, {
      x: 0.3, y: 0.82, w: 9.4, h: 0.28,
      fontSize: 9, color: GRIS_TX, italic: true, margin: 0
    });
  }
}

// ── Función helper: pie de página ─────────────────────────
function addFooter(slide) {
  slide.addText(
    `Sentinel-2 L2A (Copernicus) | GNDVI=(B8-B3)/(B8+B3) | Regresión lineal | ${config.fecha || ""}`,
    { x: 0.3, y: 5.45, w: 9.4, h: 0.18, fontSize: 7, color: "90A4AE", italic: true, margin: 0 }
  );
}

// ══════════════════════════════════════════════════════════
// SLIDE 1: Portada
// ══════════════════════════════════════════════════════════
const s0 = pres.addSlide();
s0.background = { color: VERDE_OSC };

// Logo grande centrado arriba
s0.addImage({ data: LOGO_DATA, x: 4.2, y: 0.5, w: 1.6, h: 1.2 });

// Titulo principal
s0.addText(config.titulo || "Estimacion de Rendimiento", {
  x: 0.5, y: 1.9, w: 9, h: 1.1,
  fontSize: 28, bold: true, color: BLANCO,
  align: "center", valign: "middle", margin: 0
});

// Linea naranja decorativa
s0.addShape(pres.shapes.RECTANGLE, {
  x: 3.5, y: 3.1, w: 3, h: 0.06,
  fill: { color: NARANJA }, line: { color: NARANJA }
});

// Info
const infoLines = [
  `Destino: ${config.destino || ""}  ·  Análisis GNDVI  ·  Sentinel-2`,
  `Imagen: ${config.fecha || ""}  ·  Unidad: ${config.unidad || "kg/ha"}`,
];
s0.addText(infoLines.join("\n"), {
  x: 1, y: 3.25, w: 8, h: 0.9,
  fontSize: 13, color: VERDE_CLA,
  align: "center", valign: "top", margin: 0
});

// Totales campo
const at = lotes.reduce((a,l) => a + l.area_ha, 0);
const pt = lotes.reduce((a,l) => a + l.rinde_est * l.area_ha, 0);
const rp = at > 0 ? pt / at : 0;

s0.addShape(pres.shapes.RECTANGLE, {
  x: 1.5, y: 4.3, w: 7, h: 0.95,
  fill: { color: "1B5E20", transparency: 40 }, line: { color: NARANJA, pt: 1.5 }
});
s0.addText(
  `${fmt(pt,1)} tn  ·  Rinde ponderado: ${fmt(rp,2)} ${config.unidad || "kg/ha"}  ·  Superficie: ${fmt(at,1)} ha`,
  { x: 1.5, y: 4.3, w: 7, h: 0.95, fontSize: 13, bold: true, color: BLANCO, align: "center", valign: "middle", margin: 0 }
);

// ══════════════════════════════════════════════════════════
// SLIDE 2: Resumen general
// ══════════════════════════════════════════════════════════
const s1 = pres.addSlide();
s1.background = { color: BLANCO };
addHeader(s1, "Resumen por Lote", `${config.titulo}  ·  ${config.fecha}`);

// Tabla resumen
const hdr = [
  [{text:"Lote",        options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, align:"center"}},
   {text:"Campo",       options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, align:"center"}},
   {text:"Variedad",    options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, align:"center"}},
   {text:"Sup. (ha)",   options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, align:"center"}},
   {text:"GNDVI prom.", options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, align:"center"}},
   {text:"R²",          options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, align:"center"}},
   {text:`Rinde (${config.unidad||"kg/ha"})`, options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, align:"center"}},
   {text:"Produccion (tn)", options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, align:"center"}}]
];

const rows = lotes.map((l, i) => {
  const bg = i % 2 === 0 ? GRIS_CLR : BLANCO;
  const fill = { color: bg };
  return [
    {text: l.nombre,                          options:{fill, align:"center", color:GRIS_TX}},
    {text: l.campo,                           options:{fill, align:"center", color:GRIS_TX}},
    {text: l.variedad || "-",                 options:{fill, align:"center", color:GRIS_TX}},
    {text: fmt(l.area_ha, 1),                 options:{fill, align:"center", color:GRIS_TX}},
    {text: l.idx_prom.toFixed(4),             options:{fill, align:"center", color:GRIS_TX}},
    {text: l.r2.toFixed(3)+(l.r2<0.5?" *":""),options:{fill, align:"center", color:GRIS_TX}},
    {text: fmt(l.rinde_est, 2),               options:{fill, align:"center", bold:true, color:VERDE_OSC}},
    {text: fmt(l.rinde_est * l.area_ha, 1),   options:{fill, align:"center", color:GRIS_TX}},
  ];
});

// Fila total
rows.push([
  {text:"TOTAL / PROM. POND.", options:{colspan:3, bold:true, color:BLANCO, fill:{color:VERDE_OSC}, align:"center"}},
  {text: fmt(at,1),      options:{bold:true, color:BLANCO, fill:{color:VERDE_OSC}, align:"center"}},
  {text:"",              options:{fill:{color:VERDE_OSC}}},
  {text:"",              options:{fill:{color:VERDE_OSC}}},
  {text: fmt(rp,2),      options:{bold:true, color:BLANCO, fill:{color:VERDE_OSC}, align:"center"}},
  {text: fmt(pt,1)+" tn",options:{bold:true, color:BLANCO, fill:{color:VERDE_OSC}, align:"center"}},
]);

s1.addTable([...hdr, ...rows], {
  x: 0.3, y: 1.2, w: 9.4, h: 4.1,
  fontSize: 9, border: { pt: 0.5, color: "DEE2E6" },
  colW: [1.4, 1.4, 1.1, 0.9, 1.0, 0.6, 1.2, 1.2]
});
addFooter(s1);

// ══════════════════════════════════════════════════════════
// SLIDES: Una por lote
// ══════════════════════════════════════════════════════════
lotes.forEach(function(l) {
  const sl = pres.addSlide();
  sl.background = { color: BLANCO };

  const varStr = l.variedad ? ` · Var: ${l.variedad}` : "";
  addHeader(sl,
    l.nombre,
    `Campo: ${l.campo}${varStr}  ·  Cultivo: ${l.cultivo}  ·  Sup. activa: ${fmt(l.area_ha,1)} ha`
  );

  // Imagen GNDVI (izquierda)
  if (l.mapa_b64) {
    sl.addImage({
      data: "image/png;base64," + l.mapa_b64,
      x: 0.3, y: 1.15, w: 5.8, h: 3.6
    });
  }

  // Panel derecho: datos
  const px = 6.3, pw = 3.4;

  // Titulo panel
  sl.addShape(pres.shapes.RECTANGLE, {
    x: px, y: 1.15, w: pw, h: 0.3,
    fill: { color: VERDE_MED }, line: { color: VERDE_MED }
  });
  sl.addText("Puntos de Muestreo", {
    x: px, y: 1.15, w: pw, h: 0.3,
    fontSize: 8, bold: true, color: BLANCO, align: "center", valign: "middle", margin: 0
  });

  // Tabla puntos
  if (l.puntos_datos && l.puntos_datos.length > 0) {
    const ptHdr = [[
      {text:"Ambiente", options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, fontSize:8, align:"center"}},
      {text:"GNDVI",    options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, fontSize:8, align:"center"}},
      {text:`Rinde (${config.unidad||"kg/ha"})`, options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, fontSize:8, align:"center"}},
    ]];
    const ptRows = l.puntos_datos.map((p, i) => {
      const fill = { color: i%2===0 ? GRIS_CLR : BLANCO };
      return [
        {text: p.amb,             options:{fill, fontSize:8, color:GRIS_TX}},
        {text: p.idx_val.toFixed(4), options:{fill, fontSize:8, color:GRIS_TX, align:"center"}},
        {text: fmt(p.rinde, 2),   options:{fill, fontSize:8, color:GRIS_TX, align:"center"}},
      ];
    });
    sl.addTable([...ptHdr, ...ptRows], {
      x: px, y: 1.47, w: pw, h: Math.min(1.8, 0.35 + l.puntos_datos.length * 0.28),
      border: { pt: 0.5, color: "DEE2E6" },
      colW: [1.5, 0.85, 1.0]
    });
  }

  // Modelo correlacion
  const yModel = l.puntos_datos && l.puntos_datos.length > 0 ? 1.47 + Math.min(1.8, 0.35 + l.puntos_datos.length * 0.28) + 0.12 : 1.5;

  sl.addShape(pres.shapes.RECTANGLE, {
    x: px, y: yModel, w: pw, h: 0.28,
    fill: { color: VERDE_MED }, line: { color: VERDE_MED }
  });
  sl.addText("Modelo de Correlacion", {
    x: px, y: yModel, w: pw, h: 0.28,
    fontSize: 8, bold: true, color: BLANCO, align: "center", valign: "middle", margin: 0
  });

  sl.addTable([
    [{text:"Ecuacion", options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, fontSize:8}},
     {text:"R²",       options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, fontSize:8, align:"center"}},
     {text:"GNDVI prom.", options:{bold:true, color:BLANCO, fill:{color:VERDE_MED}, fontSize:8, align:"center"}}],
    [{text: l.ecuacion,              options:{fill:{color:GRIS_CLR}, fontSize:7, color:GRIS_TX}},
     {text: l.r2.toFixed(3)+(l.r2<0.5?" *":""), options:{fill:{color:GRIS_CLR}, fontSize:8, color:GRIS_TX, align:"center"}},
     {text: l.idx_prom.toFixed(4),   options:{fill:{color:GRIS_CLR}, fontSize:8, color:GRIS_TX, align:"center"}}],
  ], {
    x: px, y: yModel + 0.3, w: pw, h: 0.55,
    border: { pt: 0.5, color: "DEE2E6" }, colW: [1.7, 0.7, 0.95]
  });

  // Caja rendimiento estimado
  sl.addShape(pres.shapes.RECTANGLE, {
    x: px, y: 4.05, w: pw, h: 1.1,
    fill: { color: VERDE_MED }, line: { color: NARANJA, pt: 2 }
  });
  sl.addText("RENDIMIENTO ESTIMADO", {
    x: px, y: 4.05, w: pw, h: 0.28,
    fontSize: 8, bold: true, color: VERDE_CLA, align: "center", valign: "middle", margin: 0
  });
  sl.addText(`${fmt(l.rinde_est, 2)} ${config.unidad || "kg/ha"}`, {
    x: px, y: 4.3, w: pw, h: 0.5,
    fontSize: 22, bold: true, color: BLANCO, align: "center", valign: "middle", margin: 0
  });
  const prod = l.rinde_est * l.area_ha;
  sl.addText(`Produccion: ${fmt(prod, 1)} tn  ·  Sup.: ${fmt(l.area_ha,1)} ha`, {
    x: px, y: 4.78, w: pw, h: 0.3,
    fontSize: 8, color: VERDE_CLA, align: "center", valign: "middle", margin: 0
  });

  addFooter(sl);
});

// Guardar
pres.writeFile({ fileName: output_path })
  .then(() => { console.log("OK:" + output_path); process.exit(0); })
  .catch(e => { console.error("ERR:" + e.message); process.exit(1); });
