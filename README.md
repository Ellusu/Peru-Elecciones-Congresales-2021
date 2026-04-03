# Peru Elecciones 2021 — Congreso y Presidencial

Análisis de resultados electorales **por mesa** (PCM) de las **Elecciones Generales de Perú 2021**: primera y segunda vuelta presidencial, y elección congresal.

## Fuente de datos

Los archivos CSV utilizados en este repositorio provienen de la **Plataforma Nacional de Datos Abiertos del Estado Peruano**:

- [https://www.datosabiertos.gob.pe/](https://www.datosabiertos.gob.pe/)

Conviene citar esa fuente en cualquier publicación o portal derivado. Los datos son de la **Oficina Nacional de Procesos Electorales (ONPE)** / catálogo de datos abiertos del Estado.

Referencias usadas en este proyecto:

- **Resultados electorales (ONPE, PCM)**: publicados vía portal de datos abiertos del Estado ([datosabiertos.gob.pe](https://www.datosabiertos.gob.pe/)).
- **Límites departamentales (GeoJSON)**: INEI, distribuido en [juaneladio/peru-geojson](https://github.com/juaneladio/peru-geojson) (archivo `dati/geo/peru_departamentos.geojson`).

## Recupero dati raw (per rigenerare il pipeline)

Per chi clona il repo: la dashboard `dashboard/index.html` funziona anche senza CSV raw, ma per rigenerare SQLite/JSON/HTML servono i file originali ONPE in `dati/`.

File attesi:

- `dati/Resultados_1ra_vuelta_Version_PCM.csv`
- `dati/Resultados_2da_vuelta*.csv` (il nome può avere suffissi/versioni diverse)
- `dati/EG2021_Congresal.csv`

Come recuperarli:

1. Vai su [datosabiertos.gob.pe](https://www.datosabiertos.gob.pe/).
2. Cerca i dataset ONPE delle **Elecciones Generales 2021** (1ª vuelta presidencial, 2ª vuelta presidencial, congresal).
3. Scarica i CSV e copiali nella cartella `dati/` con i nomi/pattern sopra.

Verifica veloce:

```bash
ls dati/Resultados_1ra_vuelta_Version_PCM.csv dati/EG2021_Congresal.csv
ls dati/Resultados_2da_vuelta*.csv
```

## Requisitos

- Python 3.9+ (recomendado)

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Pipeline de datos

Genera SQLite, resumen JSON y el bundle para la vista web:

```bash
.venv/bin/python scripts/build_elecciones_db.py
```

Salidas principales:

| Ruta | Descripción |
|------|-------------|
| `output/elecciones_2021.sqlite` | Base SQLite con métricas por departamento / provincia / distrito, ganadores, voto cruzado |
| `output/resumen_nacional.json` | Indicadores nacionales |
| `dati/diccionario_datos.json` | Nombres de candidatos y partidos (columnas `VOTOS_P*`, mapeo a `CODIGO_OP` congresal) |
| `dati/espectro_politico_2021.json` | Clasificación aproximada izquierda / centro / derecha (candidatos y listas) para gráficos comparativos |
| `dati/geo/peru_departamentos.geojson` | Límites departamentales (INEI vía [juaneladio/peru-geojson](https://github.com/juaneladio/peru-geojson)) para mapas en la vista HTML |
| `dashboard/data.json` | Datos agregados (opcional) |
| `dashboard/index.html` | Página **autónoma** con datos reales embebidos (generada por el script) |
| `dashboard/index.template.html` | Plantilla con **JSON demo** válido: puedes abrirla en el navegador para ver el diseño sin ejecutar el pipeline |

Criterio de agregación: actas con `DESCRIP_ESTADO_ACTA = CONTABILIZADA`. Los CSV deben leerse con encoding **ISO-8859-1 / latin-1**; el script corrige filas con campos vacíos extra al final (`;;`).

## Vista HTML (Apache ECharts)

- Demo online: [https://peru-elecciones-2021.darwixlab.it/#page-home](https://peru-elecciones-2021.darwixlab.it/#page-home)
- **`dashboard/index.html`**: ábrela con doble clic (datos completos tras `python scripts/build_elecciones_db.py`).
- **`dashboard/index.template.html`**: misma interfaz con datos demo vacíos; sirve para previsualizar el layout sin CSV ni SQLite.

La interfaz usa un **menú lateral** (una vista a la vez: KPIs, un gráfico o la tabla) y actualiza el hash del navegador (`#page-…`) para enlaces directos a cada pantalla. Contenido equivalente al análisis anterior: panorama nacional, votos presidenciales por candidato, listas congresales (treemap), **espectro izquierda/centro/derecha** (barras y mapas coropléticos departamentales), provincias y distritos extremos, correlación 1ª/2ª vuelta, tabla ordenable por departamento. La clasificación ideológica es editable en `dati/espectro_politico_2021.json`.

Los gráficos usan [Apache ECharts](https://echarts.apache.org/) y [echarts-gl](https://github.com/ecomfe/echarts-gl) (familia [linesGL](https://echarts.apache.org/examples/en/index.html#chart-type-linesGL)). El build sustituye el contenido de `<script type="application/json" id="bundle">` en la plantilla (no uses el marcador `__DATA_JSON__`).

## Licencia

Ver `LICENSE` del repositorio.
