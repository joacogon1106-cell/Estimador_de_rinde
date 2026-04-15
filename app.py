"""
Estimador de Rendimiento de Cultivos - Backend v4
"""
import http.server, socketserver, json, os, io, base64, struct, math, zipfile, tempfile, re

PORT = int(os.environ.get("PORT", 5050))

try:
    import numpy as np, tifffile
    from scipy import stats
    import matplotlib; matplotlib.use('Agg')
    import matplotlib.pyplot as plt, matplotlib.patheffects as pe
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import cm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                     Table, TableStyle, Image as RLImage,
                                     HRFlowable, PageBreak)
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    LIBS_OK = True; LIBS_ERR = ''
except ImportError as e:
    LIBS_OK = False; LIBS_ERR = str(e)

def _get_html():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "app.html")
    return open(p, "r", encoding="utf-8").read() if os.path.exists(p) else "<h1>No se encontro app.html</h1>"

# SHAPEFILE
def leer_dbf(data):
    n=struct.unpack('<I',data[4:8])[0]; hdr=struct.unpack('<H',data[8:10])[0]; rsz=struct.unpack('<H',data[10:12])[0]
    fields=[]; pos=32
    while data[pos]!=0x0D:
        fields.append((data[pos:pos+11].replace(b'\x00',b'').decode('latin1').strip(), data[pos+16])); pos+=32
    records=[]; rpos=hdr
    for _ in range(n):
        rec={}; fpos=1
        for name,flen in fields: rec[name]=data[rpos+fpos:rpos+fpos+flen].decode('latin1').strip(); fpos+=flen
        records.append(rec); rpos+=rsz
    return records

def leer_shp(data):
    polys=[]; pos=100
    while pos<len(data):
        clen=struct.unpack('>I',data[pos+4:pos+8])[0]*2
        if struct.unpack('<I',data[pos+8:pos+12])[0]==5:
            npts=struct.unpack('<I',data[pos+48:pos+52])[0]; nparts=struct.unpack('<I',data[pos+44:pos+48])[0]
            ps=pos+52+nparts*4; pts=[]
            for i in range(npts): pts.append((struct.unpack('<d',data[ps+i*16:ps+i*16+8])[0], struct.unpack('<d',data[ps+i*16+8:ps+i*16+16])[0]))
            polys.append(pts)
        else: polys.append(None)
        pos+=8+clen
    return polys

def pip(px,py,poly):
    n=len(poly); inside=False; j=n-1
    for i in range(n):
        xi,yi=poly[i]; xj,yj=poly[j]
        if ((yi>py)!=(yj>py)) and (px<(xj-xi)*(py-yi)/(yj-yi)+xi): inside=not inside
        j=i
    return inside

def area_ha(poly):
    n=len(poly); a=0
    for i in range(n): j=(i+1)%n; a+=poly[i][0]*poly[j][1]-poly[j][0]*poly[i][1]
    a=abs(a)/2; cy=sum(p[1] for p in poly)/n
    return a*(111320*math.cos(math.radians(cy)))*111320/10000

def norm(s): return re.sub(r'\s+',' ',str(s).strip().lower())

def col_lote(records):
    if not records: return None
    for c in ['LOTE','LOTEPLANO','LOTE_PLANO','NOMBRE','NAME','ID']:
        if c in records[0]: return c
    return list(records[0].keys())[0]

def buscar_poly(nombre, records, polys, col):
    nb=norm(nombre); nbs=nb.replace(' ','')
    cols=[c for c in [col,'LOTEPLANO','LOTE','LOTE_PLANO'] if records and c in records[0]]
    for rec,p in zip(records,polys):
        if not p: continue
        for c in cols:
            shp=norm(rec.get(c,''))
            if shp==nb or shp.replace(' ','')==nbs: return p
    for rec,p in zip(records,polys):
        if not p: continue
        for c in cols:
            shp=norm(rec.get(c,''))
            if shp and (nb in shp or shp in nb): return p
    return None

# IMAGENES
def detectar_banda(nombre):
    """Detecta la banda en nombres de archivo de Copernicus EO Browser y otros formatos."""
    n = nombre.upper()
    if not (n.endswith('.TIF') or n.endswith('.TIFF')): return None
    if 'B8A' in n or 'B08A' in n: return None
    # Formato Copernicus EO Browser: *_B03_(Raw).tiff, *_B08_(Raw).tiff
    if re.search(r'_B03[_.(]|_B3[_.(]', n): return 'B3'
    if re.search(r'_B04[_.(]|_B4[_.(]', n): return 'B4'
    if re.search(r'_B08[_.(]|_B8[_.(]', n): return 'B8'
    # Otros formatos genericos
    if '_B03' in n or 'B03_' in n: return 'B3'
    if '_B04' in n or 'B04_' in n: return 'B4'
    if '_B08' in n or 'B08_' in n: return 'B8'
    return None

def leer_tiff(data):
    arr = tifffile.imread(io.BytesIO(data)).astype(np.float32)

    # Metodo 1: tifffile nativo (mas compatible con compresion Copernicus)
    try:
        tf = tifffile.TiffFile(io.BytesIO(data))
        page = tf.pages[0]
        tags = {t.code: t.value for t in page.tags.values()}
        if 33922 in tags and 33550 in tags:
            tp = tags[33922]
            ps = tags[33550]
            return arr, float(tp[3]), float(tp[4]), float(ps[0]), float(ps[1])
    except Exception:
        pass

    # Metodo 2: lectura binaria directa
    try:
        buf = io.BytesIO(data)
        magic = buf.read(2)
        end = '<' if magic == b'II' else '>'
        buf.read(2)
        ifd_offset = struct.unpack(end+'I', buf.read(4))[0]
        buf.seek(ifd_offset)
        n_entries = struct.unpack(end+'H', buf.read(2))[0]
        tags = {}
        for _ in range(n_entries):
            tag_id = struct.unpack(end+'H', buf.read(2))[0]
            struct.unpack(end+'H', buf.read(2))
            count  = struct.unpack(end+'I', buf.read(4))[0]
            val_buf = buf.read(4)
            if tag_id in (33922, 33550):
                offset = struct.unpack(end+'I', val_buf)[0]
                pos = buf.tell()
                buf.seek(offset)
                vals = [struct.unpack(end+'d', buf.read(8))[0] for _ in range(count)]
                buf.seek(pos)
                tags[tag_id] = vals
        if 33922 in tags and 33550 in tags:
            return arr, tags[33922][3], tags[33922][4], tags[33550][0], tags[33550][1]
    except Exception:
        pass

    raise Exception(
        "No se pudo leer la georreferencia del TIFF. "
        "Descarga las bandas como 'Raw' desde Copernicus EO Browser.")

def leer_zip(zip_bytes):
    """Lee bandas del ZIP de Copernicus extrayendo a disco temporal."""
    import shutil
    b3=b4=b8=None; geo=None; log=[]; tiffs_originales=[]
    tmp_dir = tempfile.mkdtemp()
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            tiffs_originales = [n for n in zf.namelist() if n.upper().endswith(('.TIF','.TIFF'))]
            for i, name in enumerate(tiffs_originales):
                base = os.path.basename(name)
                banda = detectar_banda(base)
                if not banda:
                    log.append(f"SKIP={base}")
                    continue
                nombre_limpio = f"banda_{i}_{banda}.tiff"
                ruta_tmp = os.path.join(tmp_dir, nombre_limpio)
                try:
                    with zf.open(name) as src, open(ruta_tmp, 'wb') as dst:
                        dst.write(src.read())
                    with open(ruta_tmp, 'rb') as f:
                        data = f.read()
                    arr, olon, olat, plon, plat = leer_tiff(data)
                    g = (olon, olat, plon, plat)
                    log.append(f"{banda}={base} OK geo=({olon:.4f},{olat:.4f})")
                    if banda=='B3' and b3 is None:
                        b3=arr; geo=g
                    elif banda=='B4' and b4 is None:
                        b4=arr
                        if geo is None: geo=g
                    elif banda=='B8' and b8 is None:
                        b8=arr
                        if geo is None: geo=g
                except Exception as ex:
                    log.append(f"{banda}={base} ERR={str(ex)[:200]}")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    print(f"leer_zip resultado: {log}")
    print(f"b3={b3 is not None} b4={b4 is not None} b8={b8 is not None} geo={geo}")

    if b8 is None or geo is None:
        raise Exception(
            f"No se pudo leer las bandas del ZIP.\n"
            f"TIFFs en ZIP: {[os.path.basename(t) for t in tiffs_originales]}\n"
            f"Log detallado: {log}\n"
            f"b8={'OK' if b8 is not None else 'FALLO'}, geo={'OK' if geo is not None else 'FALLO'}")
    if b3 is None: b3=np.zeros_like(b8)
    if b4 is None: b4=np.zeros_like(b8)
    return b3,b4,b8,geo

def calc_idx(b3,b4,b8,indice):
    r={}
    with np.errstate(invalid='ignore',divide='ignore'):
        if indice in ('ndvi','ambos'): r['NDVI']=np.where((b8+b4)>0,(b8-b4)/(b8+b4),np.nan)
        if indice in ('gndvi','ambos'): r['GNDVI']=np.where((b8+b3)>0,(b8-b3)/(b8+b3),np.nan)
    return r

def val_punto(img,lat,lon,olat,olon,plat,plon):
    rows,cols=img.shape; c=int((lon-olon)/plon); r=int((olat-lat)/plat)
    r0,r1=max(0,r-1),min(rows,r+2); c0,c1=max(0,c-1),min(cols,c+2)
    if r0>=r1 or c0>=c1: return None
    v=float(np.nanmean(img[r0:r1,c0:c1])); return v if np.isfinite(v) else None

def val_lote(img,poly,olat,olon,plat,plon):
    rows,cols=img.shape
    lons=[p[0] for p in poly]; lats=[p[1] for p in poly]
    c0=max(0,int((min(lons)-olon)/plon)-2); c1=min(cols-1,int((max(lons)-olon)/plon)+2)
    r0=max(0,int((olat-max(lats))/plat)-2); r1=min(rows-1,int((olat-min(lats))/plat)+2)
    vals=[]
    for r in range(r0,r1+1):
        for c in range(c0,c1+1):
            if pip(olon+c*plon, olat-r*plat, poly):
                v=img[r,c]
                if np.isfinite(v) and v>0: vals.append(float(v))
    return (float(np.mean(vals)),len(vals)) if vals else (None,0)

# MAPA
def gen_mapa(img,poly,puntos,olat,olon,plat,plon,titulo,idx_nom,unidad):
    rows,cols=img.shape; mg=0.003
    lons=[p[0] for p in poly]; lats=[p[1] for p in poly]
    c0=max(0,int((min(lons)-mg-olon)/plon)); c1=min(cols-1,int((max(lons)+mg-olon)/plon)+1)
    r0=max(0,int((olat-max(lats)-mg)/plat)); r1=min(rows-1,int((olat-min(lats)+mg)/plat)+1)
    crop=img[r0:r1+1,c0:c1+1].copy(); h,w=crop.shape
    mask=np.zeros((h,w),dtype=bool)
    for r in range(h):
        for c in range(w):
            if pip(olon+(c0+c)*plon, olat-(r0+r)*plat, poly): mask[r,c]=True
    crop[~mask]=np.nan
    ext=[olon+c0*plon,olon+c1*plon,olat-r1*plat,olat-r0*plat]
    fig,ax=plt.subplots(figsize=(7,6),facecolor='white')
    cm2=plt.cm.RdYlGn.copy(); cm2.set_bad('#e8e8e8')
    im=ax.imshow(crop,cmap=cm2,vmin=0.5,vmax=1.0,extent=ext,origin='upper',interpolation='nearest')
    ax.plot([p[0] for p in poly]+[poly[0][0]],[p[1] for p in poly]+[poly[0][1]],'k-',lw=1.5,zorder=5)
    clrs=['#1565C0','#E65100','#2E7D32','#6A1B9A','#AD1457','#00838F']
    for i,(amb,lat,lon,rinde) in enumerate(puntos):
        cp=clrs[i%len(clrs)]; ax.scatter(lon,lat,s=130,c=cp,zorder=10,edgecolors='white',linewidth=1.5)
        ax.annotate(f"{amb}\n{rinde} {unidad}",xy=(lon,lat),xytext=(8,8),textcoords='offset points',
                    fontsize=7.5,fontweight='bold',color=cp,
                    path_effects=[pe.withStroke(linewidth=2,foreground='white')],zorder=11)
    cb=plt.colorbar(im,ax=ax,fraction=0.03,pad=0.02); cb.set_label(idx_nom,fontsize=9); cb.ax.tick_params(labelsize=8)
    ax.set_xlabel('Longitud',fontsize=8); ax.set_ylabel('Latitud',fontsize=8); ax.tick_params(labelsize=7)
    ax.set_title(titulo,fontsize=10,fontweight='bold',pad=8); plt.tight_layout()
    buf=io.BytesIO(); plt.savefig(buf,dpi=150,bbox_inches='tight',facecolor='white'); plt.close(); buf.seek(0)
    return buf

# PDF
def gen_pdf(lotes,config,path):
    from datetime import datetime
    GD=colors.HexColor('#1B5E20'); GM=colors.HexColor('#2E7D32'); GC=colors.HexColor('#C8E6C9')
    GL=colors.HexColor('#F5F5F5'); LN=colors.HexColor('#CFD8DC'); OR=colors.HexColor('#E65100')
    WH=colors.white; GT=colors.HexColor('#37474F')
    def s(n,**k):
        d=dict(fontName='Helvetica',fontSize=10,textColor=GT,leading=14,spaceAfter=0,spaceBefore=0); d.update(k); return ParagraphStyle(n,**d)
    s_tit=s('t',fontSize=20,textColor=GD,fontName='Helvetica-Bold',alignment=TA_CENTER,leading=26,spaceAfter=8)
    s_sub=s('sb',fontSize=11,alignment=TA_CENTER,leading=15,spaceAfter=4)
    s_fch=s('f',fontSize=9,textColor=colors.HexColor('#78909C'),alignment=TA_CENTER,leading=13)
    s_sec=s('sc',fontSize=10,fontName='Helvetica-Bold',textColor=GM,leading=14,spaceAfter=5,spaceBefore=8)
    s_not=s('n',fontSize=8,fontName='Helvetica-Oblique',textColor=colors.HexColor('#78909C'),leading=11,spaceAfter=3)
    s_fot=s('fo',fontSize=7.5,textColor=colors.HexColor('#90A4AE'),alignment=TA_CENTER,leading=11)
    s_rl=s('rl',fontSize=10,fontName='Helvetica-Bold',textColor=WH,alignment=TA_CENTER,leading=14)
    s_rv=s('rv',fontSize=16,fontName='Helvetica-Bold',textColor=WH,alignment=TA_CENTER,leading=22)
    s_pl=s('pl',fontSize=10,fontName='Helvetica-Bold',textColor=WH,alignment=TA_CENTER,leading=14)
    s_pv=s('pv',fontSize=13,fontName='Helvetica-Bold',textColor=WH,alignment=TA_CENTER,leading=18)
    HDR=[('BACKGROUND',(0,0),(-1,0),GM),('TEXTCOLOR',(0,0),(-1,0),WH),('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),
         ('FONTSIZE',(0,0),(-1,-1),9),('ALIGN',(0,0),(-1,-1),'CENTER'),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
         ('FONTNAME',(0,1),(-1,-1),'Helvetica'),('ROWBACKGROUNDS',(0,1),(-1,-1),[GL,WH]),('GRID',(0,0),(-1,-1),0.4,LN),
         ('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),('LEFTPADDING',(0,0),(-1,-1),8),('RIGHTPADDING',(0,0),(-1,-1),8)]
    def tbl(data,cw,ex=None):
        t=Table(data,colWidths=cw,repeatRows=1); t.setStyle(TableStyle(list(HDR)+(ex or []))); return t
    indice=config['indice'].upper(); unidad=config['unidad']; now=datetime.now().strftime('%d/%m/%Y')
    doc=SimpleDocTemplate(path,pagesize=A4,leftMargin=2*cm,rightMargin=2*cm,topMargin=1.8*cm,bottomMargin=1.8*cm)
    W=A4[0]-4*cm; story=[]
    story+=[HRFlowable(width=W,thickness=4,color=GD,spaceAfter=12),Paragraph(config['titulo'],s_tit),
            Paragraph(f"Destino: {config['destino']} &nbsp;&middot;&nbsp; Analisis {indice} &nbsp;&middot;&nbsp; Sentinel-2",s_sub),
            Spacer(1,4),Paragraph(f"Imagen: {config['fecha']} &nbsp;&middot;&nbsp; Generado: {now}",s_fch),
            Spacer(1,12),HRFlowable(width=W,thickness=2,color=GD,spaceAfter=14)]
    story.append(Paragraph('Resumen por Lote',s_sec))
    dr=[['Lote','Campo','Cultivo','Sup. (ha)',indice+' prom.','R²',f'Rinde ({unidad})']]
    for l in lotes:
        dr.append([l['nombre'],l['campo'],l['cultivo'],f"{l['area_ha']:.2f}",f"{l['idx_prom']:.4f}",
                   f"{l['r2']:.3f}"+(' *' if l['r2']<0.5 else ''),f"{l['rinde_est']:.3f}"])
    story.append(tbl(dr,[2.7*cm,3*cm,1.9*cm,2*cm,2.2*cm,1.3*cm,2.9*cm],
                     [('TEXTCOLOR',(6,1),(6,-1),GD),('FONTNAME',(6,1),(6,-1),'Helvetica-Bold')]))
    story.append(Spacer(1,5))
    if any(l['r2']<0.5 for l in lotes): story.append(Paragraph('* R² bajo: correlacion debil.',s_not))
    story.append(Spacer(1,16))
    story+=[HRFlowable(width=W,thickness=1,color=LN,spaceAfter=12),Paragraph('Produccion Total Estimada',s_sec)]
    at=sum(l['area_ha'] for l in lotes); pt=sum(l['rinde_est']*l['area_ha'] for l in lotes); rp=pt/at if at else 0
    dp=[['Lote','Superficie (ha)',f'Rinde ({unidad})','Produccion (tn)']]
    for l in lotes: dp.append([l['nombre'],f"{l['area_ha']:.2f}",f"{l['rinde_est']:.3f}",f"{l['rinde_est']*l['area_ha']:.1f}"])
    dp.append(['TOTAL / PROM. POND.',f"{at:.2f}",f"{rp:.3f}",f"{pt:.1f}"])
    story.append(tbl(dp,[4.5*cm,3.5*cm,3.5*cm,4.5*cm],
                     [('BACKGROUND',(0,-1),(-1,-1),GD),('TEXTCOLOR',(0,-1),(-1,-1),WH),('FONTNAME',(0,-1),(-1,-1),'Helvetica-Bold')]))
    story.append(Spacer(1,16))
    cx=Table([[Paragraph('PRODUCCION TOTAL ESTIMADA',s_pl)],
              [Paragraph(f"{pt:.1f} tn &nbsp;|&nbsp; Rinde pond.: {rp:.3f} {unidad} &nbsp;|&nbsp; Sup.: {at:.2f} ha",s_pv)]],colWidths=[W])
    cx.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),GD),('TOPPADDING',(0,0),(0,0),9),('BOTTOMPADDING',(0,0),(0,0),4),
                             ('TOPPADDING',(0,1),(0,1),4),('BOTTOMPADDING',(0,1),(0,1),10),
                             ('LEFTPADDING',(0,0),(-1,-1),12),('RIGHTPADDING',(0,0),(-1,-1),12),('LINEABOVE',(0,0),(-1,0),3,OR)]))
    story.append(cx)
    IW=W; IH=W*0.60
    for l in lotes:
        story.append(PageBreak())
        ht=Table([[Paragraph(f"<b>{l['nombre']}</b>",s('lh',fontSize=14,fontName='Helvetica-Bold',textColor=WH,alignment=TA_LEFT,leading=18)),
                   Paragraph(f"Campo: {l['campo']} &nbsp;|&nbsp; Cultivo: {l['cultivo']} &nbsp;|&nbsp; Sup.: {l['area_ha']:.2f} ha",
                              s('lhi',fontSize=9,textColor=GC,alignment=TA_LEFT,leading=13))]],colWidths=[4*cm,W-4*cm])
        ht.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),GD),('VALIGN',(0,0),(-1,-1),'MIDDLE'),
                                 ('TOPPADDING',(0,0),(-1,-1),10),('BOTTOMPADDING',(0,0),(-1,-1),10),
                                 ('LEFTPADDING',(0,0),(-1,-1),12),('RIGHTPADDING',(0,0),(-1,-1),8),('LINEBELOW',(0,0),(-1,-1),3,OR)]))
        story+=[ht,Spacer(1,10),Paragraph(f'Imagen {indice} y Puntos de Muestreo',s_sec)]
        l['mapa_buf'].seek(0); story.append(RLImage(l['mapa_buf'],width=IW,height=IH)); story.append(Spacer(1,10))
        pd2=[['Ambiente',indice,f'Rinde ({unidad})']]
        for p in l['puntos_datos']: pd2.append([p['amb'],f"{p['idx_val']:.4f}",f"{p['rinde']:.3f}"])
        story.append(tbl(pd2,[6*cm,5*cm,6*cm])); story.append(Spacer(1,12))
        story+=[HRFlowable(width=W,thickness=1,color=LN,spaceAfter=8),Paragraph(f'Modelo de Correlacion {indice} - Rendimiento',s_sec)]
        dm=[['Ecuacion','R²',f'{indice} prom.'],[l['ecuacion'],f"{l['r2']:.3f}"+(' *' if l['r2']<0.5 else ''),f"{l['idx_prom']:.4f}"]]
        story.append(tbl(dm,[8*cm,2.5*cm,5.5*cm])); story.append(Spacer(1,6))
        if l['r2']<0.5: story.append(Paragraph('* R² bajo: interpretar con precaucion.',s_not))
        story.append(Spacer(1,6))
        rt=Table([[Paragraph('RENDIMIENTO ESTIMADO',s_rl)],
                  [Paragraph(f"{l['rinde_est']:.3f} {unidad} &nbsp;|&nbsp; {l['rinde_est']*1000:.0f} kg/ha",s_rv)]],colWidths=[W])
        rt.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,-1),GM),('TOPPADDING',(0,0),(0,0),8),('BOTTOMPADDING',(0,0),(0,0),4),
                                 ('TOPPADDING',(0,1),(0,1),4),('BOTTOMPADDING',(0,1),(0,1),10),
                                 ('LEFTPADDING',(0,0),(-1,-1),12),('RIGHTPADDING',(0,0),(-1,-1),12),('LINEABOVE',(0,0),(-1,0),3,OR)]))
        story+=[rt,Spacer(1,12),HRFlowable(width=W,thickness=0.5,color=LN,spaceAfter=5),
                Paragraph(f'Sentinel-2 L2A (Copernicus) | {indice} | Regresion lineal | {now}',s_fot)]
    doc.build(story)

# HTTP
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,fmt,*a): pass
    def cors(self):
        self.send_header('Access-Control-Allow-Origin','*')
        self.send_header('Access-Control-Allow-Methods','POST, GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers','Content-Type')
    def do_OPTIONS(self): self.send_response(200); self.cors(); self.end_headers()
    def do_GET(self):
        if self.path=='/status':
            self.send_response(200); self.send_header('Content-Type','application/json'); self.cors(); self.end_headers()
            r={'ok':LIBS_OK}
            if not LIBS_OK: r['error']=LIBS_ERR
            self.wfile.write(json.dumps(r).encode())
        elif self.path in ('/', '/app', '/app.html', '/index.html'):
            html=_get_html(); self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.cors(); self.end_headers()
            self.wfile.write(html.encode('utf-8'))

    def do_POST(self):
        length=int(self.headers.get('Content-Length',0)); body=self.rfile.read(length)
        try:
            result=self.procesar(json.loads(body))
            self.send_response(200); self.send_header('Content-Type','application/json'); self.cors(); self.end_headers()
            self.wfile.write(json.dumps(result).encode())
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            print("\n=== ERROR ===")
            print(tb)
            print("=============\n")
            self.send_response(500); self.send_header('Content-Type','application/json'); self.cors(); self.end_headers()
            self.wfile.write(json.dumps({'error':str(e),'detalle':tb}).encode())
    def procesar(self,payload):
        if not LIBS_OK: raise Exception(f'Falta libreria: {LIBS_ERR}. Ejecuta ARRANCAR.bat.')
        config=payload['config']; grupos=payload['grupos']
        records=leer_dbf(base64.b64decode(payload['dbf']))
        polys=leer_shp(base64.b64decode(payload['shp']))
        cl=col_lote(records)
        b3,b4,b8,geo=leer_zip(base64.b64decode(payload['zip']))
        print(f"DEBUG leer_zip retorno: b3={b3 is not None}, b4={b4 is not None}, b8={b8 is not None}, geo={geo}")
        if geo is None:
            raise Exception("geo es None despues de leer_zip - el ZIP no pudo ser leido correctamente")
        olon,olat,plon,plat=geo
        print(f"DEBUG geo: olon={olon}, olat={olat}, plon={plon}, plat={plat}")
        indices=calc_idx(b3,b4,b8,config['indice'])
        idx_nom='GNDVI' if config['indice']=='gndvi' else 'NDVI' if config['indice']=='ndvi' else 'GNDVI'
        img=indices.get('GNDVI',indices.get('NDVI'))
        print(f"DEBUG indices keys: {list(indices.keys())}, img is None: {img is None}")
        if img is None:
            raise Exception(f"No se pudo calcular el indice {config['indice']}. Verificá que las bandas correctas esten en el ZIP.")
        lotes_res=[]
        for grupo in grupos:
            pts=[]
            for pt in grupo['puntos']:
                try:
                    lat_v = float(pt['lat'])
                    lon_v = float(pt['lon'])
                    rinde_v = float(pt['rinde'])
                except (TypeError, ValueError):
                    print(f"Punto ignorado (lat/lon/rinde invalido): {pt}")
                    continue
                import math
                if math.isnan(lat_v) or math.isnan(lon_v):
                    print(f"Punto ignorado (NaN): {pt}")
                    continue
                # Auto-corregir si lat y lon parecen estar invertidos (zona Argentina)
                if -40 > lat_v > -80 and -20 > lon_v > -40:
                    pass  # ok
                elif -40 > lon_v > -80 and -20 > lat_v > -40:
                    lat_v, lon_v = lon_v, lat_v  # invertir
                    print(f"Auto-corregido lat/lon para punto {pt['amb']}")
                v=val_punto(img, lat_v, lon_v, olat, olon, plat, plon)
                if v is not None:
                    pts.append({'amb':pt['amb'],'lat':lat_v,'lon':lon_v,'rinde':rinde_v,'idx_val':v})
            if len(pts)<2: raise Exception(f'El grupo "{grupo["nombre"]}" necesita al menos 2 puntos validos en la imagen.')
            xg=np.array([p['idx_val'] for p in pts]); yg=np.array([p['rinde'] for p in pts])
            sl,it,r,_,_=stats.linregress(xg,yg); r2=r**2
            for lote in grupo['lotes']:
                poly=buscar_poly(lote['nombre'],records,polys,cl)
                if poly is None:
                    disp=list(set(rec.get(cl,'') for rec in records if rec.get(cl,'')))
                    raise Exception(f'Lote "{lote["nombre"]}" no encontrado.\nColumna: "{cl}".\nDisponibles: {disp[:25]}')
                ha=area_ha(poly); prom,_=val_lote(img,poly,olat,olon,plat,plon)
                if prom is None: raise Exception(f'Sin pixeles validos en lote "{lote["nombre"]}".')
                rinde=sl*prom+it; ec=f"y = {sl:.2f} x {idx_nom} + ({it:.2f})"
                mb=gen_mapa(img,poly,[(p['amb'],p['lat'],p['lon'],p['rinde']) for p in pts],olat,olon,plat,plon,
                            f"{idx_nom} - {lote['nombre']}",idx_nom,config['unidad'])
                lotes_res.append({'nombre':lote['nombre'],'campo':lote['campo'],'cultivo':lote['cultivo'],
                                  'area_ha':ha,'idx_prom':prom,'r2':r2,'rinde_est':rinde,'ecuacion':ec,
                                  'puntos_datos':pts,'mapa_buf':mb})
        with tempfile.NamedTemporaryFile(suffix='.pdf',delete=False) as tmp: pdf_path=tmp.name
        gen_pdf(lotes_res,config,pdf_path)
        with open(pdf_path,'rb') as f: b64=base64.b64encode(f.read()).decode()
        os.unlink(pdf_path)
        return {'ok':True,'pdf':b64}

if __name__=='__main__':
    print(f'Servidor iniciando en puerto {PORT}')
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(('0.0.0.0', PORT), Handler) as srv:
        srv.serve_forever()
