# SQL Analytics Toolkit

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-0.8-yellow)
![License](https://img.shields.io/badge/license-MIT-green)

Estuve un tiempo trabajando con datos, después me fui a otro rubro y cuando quise volver me di cuenta de que el mundo había cambiado bastante. Este proyecto es parte de mi proceso de ponerme al día — practicar los análisis que realmente se usan en una empresa, no los ejemplos de tutoriales que nunca se parecen a la realidad.

Elegí un caso de e-commerce porque es el contexto donde más se aplican estos patrones: cohortes, RFM, funnels, window functions. Y usé DuckDB porque es la herramienta que más está creciendo en el mundo de Data Engineering hoy, especialmente para análisis local sobre archivos Parquet.

---

## Qué hace este proyecto

Sobre datos sintéticos de e-commerce (500 clientes, 3.000 órdenes, 6 países de LATAM) implementé 7 análisis y un simulador de impacto:

| Análisis | Para qué sirve |
|---|---|
| **Cohort Retention** | Ver qué % de usuarios volvió a comprar mes a mes |
| **RFM Segmentation** | Clasificar clientes en Champions, At Risk, Lost, etc. |
| **Conversion Funnel** | Encontrar dónde se van los usuarios antes de comprar |
| **DAU / WAU / MAU** | Medir actividad diaria, semanal y mensual |
| **Revenue Breakdown** | Revenue por categoría con crecimiento mes a mes |
| **Window Functions** | Rankings, percentiles, running totals, LAG/LEAD |
| **Simulador de impacto** | Cuantificar cuánto revenue extra generaría mejorar el funnel |

El simulador fue lo más interesante de construir — en vez de solo mostrar el problema (19% de conversión final), modela 4 escenarios de mejora y calcula el revenue incremental de cada uno. Es la diferencia entre un análisis descriptivo y uno que le sirve a alguien para tomar una decisión.

---

## Instalación

```bash
git clone https://github.com/DatabyFede/Sql-analytics-toolkit.git
cd Sql-analytics-toolkit
pip install duckdb pandas matplotlib seaborn plotly
```

---

## Uso

```python
from toolkit import SQLAnalyticsToolkit

tk = SQLAnalyticsToolkit()
tk.seed_demo_data()

print(tk.executive_summary())
print(tk.rfm_segmentation().head(10))
print(tk.conversion_funnel())
```

O bien, se puede visualizar todo en el notebook:

```bash
jupyter notebook demo_notebook.ipynb
```

---

## Estructura

```
sql-analytics-toolkit/
├── toolkit.py            # Clase principal con todos los análisis
├── demo_notebook.ipynb   # Notebook con visualizaciones
├── requirements.txt
└── README.md
```

---

## Lo que aprendí (o re-aprendí)

Las window functions son de esas cosas que uno lee y cree entender hasta que las tiene que aplicar en un caso real. El análisis de cohortes me hizo pensar bastante — la retención no cae linealmente como uno intuye, sube en el mes 2-3 para los usuarios que sobreviven el primer mes, lo que cambia cómo pensás la estrategia de retención.

También me sorprendió lo útil que es DuckDB para este tipo de análisis. Corre SQL directamente sobre DataFrames de pandas sin necesitar ningún servidor, y es notablemente más rápido que SQLite para consultas analíticas.

---

## Por qué usé estas herramientas

**DuckDB** en lugar de SQLite o pandas puro — quería practicar con algo que se está usando en la industria hoy, especialmente en stacks modernos de Data Engineering con Parquet y S3.

**Datos sintéticos** en lugar de un dataset de Kaggle — generarlos me obligó a pensar en cómo se distribuyen los datos reales: distribuciones de montos, drop-off por etapa del funnel, patrones de retención. Más útil que copiar un CSV.

---

## Próximos pasos

- [ ] Conectar a datos reales del BCRA o INDEC
- [ ] Exportar resultados a Parquet con DuckDB
- [ ] Deploy como app Streamlit con filtros interactivos

---

## Autor

**DatabyFede** · [LinkedIn](https://www.linkedin.com/in/federico-matyjaszczyk/) · [GitHub](https://github.com/DatabyFede)

> Parte de mi portfolio de proyectos de Data & IA — armado para volver al ruedo después de un tiempo fuera del mundo de los datos.
